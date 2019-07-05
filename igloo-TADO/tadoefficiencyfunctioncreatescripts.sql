create function tado_estimate_mean_internal_temp(setpoint double precision, heating_hours double precision) returns double precision
	stable
	language plpythonu
as $$
  import logging
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)
  import numpy as np

  HLC= 250.0
  TIMECONSTANT= 50.0
  EXTTEMP= 7.0
  HEATCAPACITY= 0.11 * 100

  unheated_hours=24.0-heating_hours

  unheated_heat_loss = HEATCAPACITY * (setpoint - EXTTEMP) * (1.-np.exp(-unheated_hours/TIMECONSTANT))
  mit_unheated = EXTTEMP + unheated_heat_loss * 1000000 / (HLC * unheated_hours * 3600)

  mit = ((setpoint * heating_hours) + (mit_unheated * unheated_hours)) / 24.0
  return mit
$$
;

create function tado_heating_summary(custom_values character varying) returns character varying
	stable
	language plpythonu
as $$
    import json
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    hs = 'unknown'
    if custom_values != '':
    		json_values = json.loads(custom_values)
    else:
    		return hs
    ages = []
    for d in json_values:
        if d['age'] == '0 to 9':
            ages.append(1)
        if d['age'] == '10 to 17':
            ages.append(2)
        if d['age'] == '18 to 64':
            ages.append(3)
        if d['age'] == '65 to 74':
            ages.append(4)
        if d['age'] == '75 and over':
            ages.append(5)

    no_oc = len(ages)

    if no_oc == 0:
        ft = 'unknown_no_occupants_reported'
        hs = 'unknown'
    else:
        if no_oc == 1:
            if ages[0] == 3:
                ft = 'working_age_individual'
                hs = 'working_no_kids'
            elif ages[0] > 3:
                ft = 'retired_individual'
                hs = 'retired'
            else:
                ft = 'unknown_bad_age_data'
                hs = 'unknown'

        elif no_oc == 2:
            if all([a == 3 for a in ages]):
                ft = 'working_age_couple'
                hs = 'working_no_kids'

            elif sum(ages) > 6:
                ft = 'retired_couple'
                hs = 'retired'

            elif sum(ages) < 6:
                if 1 in ages or 2 in ages and 3 in ages:
                    ft = 'single_parent'
                    hs = 'working_kids'

                elif 1 in ages or 2 in ages and 3 in ages or 4 in ages:
                    ft = 'retired_parent'
                    hs = 'retired'

                else:
                    ft = 'undefined_type_2'
                    hs = 'unknown'

        elif no_oc == 3:
            if all([a == 3 for a in ages]):
                ft = 'working_house_share_3'
                hs = 'working_no_kids'

            elif sum(ages) > 12:
                ft = 'retired_3'
                hs = 'retired'
            else:
                ft = 'working_age_family_3'
                hs = 'working_kids'

        elif no_oc == 4:
            if all([a == 3 for a in ages]):
                ft = 'working_house_share_4'
                hs = 'working_no_kids'

            elif sum(ages) > 16:
                ft = 'retired_4'
                hs = 'retired'

            else:
                ft = 'working_age_family_4'
                hs = 'working_kids'

        else:
            if all([a == 3 for a in ages]):
                ft = 'working_house_share_5plus'
                hs = 'working_no_kids'

            elif sum(ages) > 20:
                ft = 'retired_5plus'
                hs = 'retired'

            else:
                ft = 'working_age_family_5plus'
                hs = 'working_kids'

    return hs

$$
;

create function tado_estimate_heating_hours(heating_type_input character varying, type character varying) returns double precision
	stable
	language plpythonu
as $$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    heating_hours_assumption = {'working_no_kids': [16, 10],
                                'working_kids': [16, 12],
                                'retired': [16, 14],
                                'unknown': [16, 13]}

    heating_type = heating_type_input
    if type == 'base':
      if heating_type in heating_hours_assumption.keys():
          return heating_hours_assumption[heating_type][0]
      else:
          return heating_hours_assumption['unknown'][0]
    if type == 'estimate':
      if heating_type in heating_hours_assumption.keys():
          return heating_hours_assumption[heating_type][1]
      else:
          return heating_hours_assumption['unknown'][1]
$$
;

create function tado_estimate_setpoint_impact(setpoint_input double precision, control_input character varying, heating_input character varying) returns double precision
	stable
	language plpythonu
as $$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    BASE_TEMP = 20.0
    all_defaults = True

    setpoint = setpoint_input

    setpoint = float(setpoint)

    # deal with missing setpoint info
    if not setpoint > 0.:
        setpoint = 20.
    else:
        all_defaults = False

    setpoint_impact = (BASE_TEMP - setpoint) * 0.25  # was 0.25

    # ADJUST FOR CURRENT CONTROL APPROACH
    control_assumption = {'smartthermostat': 0.,
                        'thermostatautomatic': -0.5,
                        'timerprogrammer': -0.25,
                        'manually': 0.25,
                        'thermostatmanual': -0.25,
                        'nocontrol': -0.5}

    control_approach = control_input

    if control_approach in control_assumption.keys():
        control_impact = control_assumption[control_approach]
        all_defaults = False
    else:
        control_impact = 0.

    # ADJUST FOR HEATING EXTENT
    heating_extent_assumption = {'wholehome': 0.,
                               'specificrooms': +0.5}

    heating_extent = heating_input

    if heating_extent in heating_extent_assumption.keys():
        heating_extent_impact = heating_extent_assumption[heating_extent]
        all_defaults = False
    else:
        heating_extent_impact = 0.

    sp_impact = setpoint_impact + control_impact + heating_extent_impact
    sp_base = setpoint + sp_impact
    return sp_base
$$
;

create function tado_savings_segmentation(mmh_tado_status character varying, fuel_type character varying, heating_control character varying, heating_source character varying, pounds_savings double precision) returns integer
	stable
	language plpythonu
as $$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    segment_id = -1

    if heating_control == 'smartthermostat':
      return 4

    if heating_source not in ('gasboiler', 'oilboiler', 'unknown'):
      return 5

    if fuel_type in ('EG', 'GE'):
      fuel_type = 'B'

    if mmh_tado_status == 'complete':
      if pounds_savings < 0:
        segment_id = 1
      if pounds_savings >= 0:
        segment_id = 2

    if mmh_tado_status in ('incomplete', 'nodata'):
      if fuel_type == 'E':
        segment_id = 6
      if fuel_type in ('G', 'B'):
        segment_id = 3

    return segment_id
$$
;

create function tado_get_heating_source(heating_source character varying) returns character varying
	stable
	language plpythonu
as $$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    mmh_heating_source = 'unknown'

    if heating_source in ('gasboiler', 'gasfires'):
      mmh_heating_source = 'gas'
    if heating_source in ('oilboiler', 'biomassboiler', 'solidfuel'):
      mmh_heating_source = 'other'
    if heating_source in ('electricstorage', 'electricheatpump', 'electricradiators'):
      mmh_heating_source = 'electricity'

    return mmh_heating_source
$$
;

create function tado_get_prop_age_id(house_age character varying) returns integer
	stable
	language plpythonu
as $$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    mmh_house_age = -99

    if house_age in ('<1850', '1850to1899', '1900to1918', '1919to1930'):
      return 101
    if house_age == '1931to1944':
      return 102
    if house_age == '1945to1964':
		  return 103
    if house_age == '1965to1980':
      return 104
    if house_age in ('1981to1990', '1991to1995'):
      return 105
    if house_age in ('1996to2001', '>2002'):
      return 106

    return mmh_house_age


$$
;

create function tado_get_prop_floor_area_band(house_bedrooms character varying, house_type character varying) returns integer
	stable
	language plpythonu
as $$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    house_floor_area_band = -99

    if house_bedrooms == 'one_bedroom':
      return 1
    if house_bedrooms == 'two_bedroom':
      return 2
    if house_bedrooms == 'three_bedroom':
      return 2
    if house_bedrooms == 'four_bedroom':
      if house_type == 'flat':
        return 2
      else:
        return 3
    if house_bedrooms == 'fiveplus_bedroom':
      if house_type in ('converted_flat', 'flat', 'mid_terrace', 'end_terrace'):
        return 3
      else:
        return 4

    return house_floor_area_band

$$
;

create function tado_get_prop_type_id(house_type character varying) returns integer
	stable
	language plpythonu
as $$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    mmh_house_type = -99

    if house_type == 'detached_house':
      return 101
    if house_type == 'semidetached_house':
      return 102
    if house_type == 'end_terrace':
      return 103
    if house_type == 'mid_terrace':
      return 104
    if house_type == 'bungalow':
      return 105
    if house_type in ('converted_flat', 'flat'):
      return 106

    return mmh_house_type

$$
;

