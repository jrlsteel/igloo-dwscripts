--Batch SQL for igloo-TADO
select x1.*,
       tado_estimate_mean_internal_temp(coalesce(x1.base_temp,0.0), coalesce(x1.base_hours, 0.0)) as base_mit,
       tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp,0.0), coalesce(x1.estimate_hours,0.0)) as est_mit,
       aq as AQ,
       unit_Rate as unit_rate,
       aq * unit_Rate/100 amount_over_year,
        (
            (
              tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp,0.0), coalesce(x1.estimate_hours,0.0)) -
              tado_estimate_mean_internal_temp(coalesce(x1.base_temp,0.0), coalesce(x1.base_hours, 0.0))
            ) /
            tado_estimate_mean_internal_temp(coalesce(x1.base_temp,0.0), coalesce(x1.base_hours, 0.0)) * 100
        ) as perc_diff,
       (abs(
            (
              tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp,0.0), coalesce(x1.estimate_hours,0.0)) -
              tado_estimate_mean_internal_temp(coalesce(x1.base_temp,0.0), coalesce(x1.base_hours, 0.0))
            ) /
            tado_estimate_mean_internal_temp(coalesce(x1.base_temp,0.0), coalesce(x1.base_hours, 0.0))
        ) * aq * (unit_Rate/100))  savings_in_pounds


from (
select
x.user_id,
x.account_id,
cast (max(x.temperature_preference) as double precision) base_temp,
max(heating_control_type) as heating_control,
max(x.heating_basis) as heating_basis,
max(x.heating_type) as heating_type,
max(x.heating_source) as heating_source,
max(x.custom_value) as ages,
max(x.status) as status,
max(coalesce(registers_eacaq, 0)) as aq,
max(coalesce(tf.rate, 0)) as unit_Rate,
max(q.gas_usage),
tado_estimate_setpoint_impact(max(x.temperature_preference),max(heating_control_type),max(x.heating_basis)) as estimated_temp,
tado_estimate_heating_hours(max(x.heating_type), 'base') as base_hours,
tado_estimate_heating_hours(max(x.heating_type), 'estimate') as estimate_hours
from (

select
u.id as user_id,
su.external_id as account_id,
att.id as attribute_type_id,
at.entity_type,
att.attribute_name as attribute_name,
att.attribute_description,
att.attribute_fixed as fixed,
sq.title,
av.attribute_value as fixed_value,
case when att.attribute_name='temperature_preference' then
    av.attribute_value else '20' end as temperature_preference,
case when att.attribute_name='heating_basis' then
    av.attribute_value else '' end as heating_basis,
case when att.attribute_name='heating_control_type' then
    av.attribute_value else '' end as heating_control_type,
case when att.attribute_name='heating_type' then
    av.attribute_value else '' end as heating_source,
at.attribute_custom_value as custom_value,
sr.status as status,
case when att.attribute_name = 'resident_ages' then
 tado_heating_summary(coalesce(at.attribute_custom_value, ''))
  else '' end as heating_type
from
ref_cdb_supply_contracts su
       inner join ref_cdb_user_permissions up on su.id = up.permissionable_id and permission_level = 0
                                                     and permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_users u on u.id = up.user_id
       inner join ref_cdb_attributes at on (
                                            (at.entity_id = up.user_id AND at.entity_type = 'App\\User')
                                                      OR
                                            (at.entity_id = su.supply_address_id AND at.entity_type = 'App\\Address')
                                            )
                                        and at.effective_to is null
       inner join ref_cdb_attribute_types att on att.id = at.attribute_type_id and att.effective_to is null
       left outer join ref_cdb_attribute_values av on av.attribute_type_id = at.attribute_type_id and at.attribute_value_id = av.id
                                                        and at.attribute_value_id is not null
       inner join ref_cdb_survey_questions sq on sq.attribute_type_id = att.id
       inner join ref_cdb_survey_category sc on sc.id = sq.survey_category_id
       inner join ref_cdb_survey_response sr on sr.user_id = up.user_id and sr.survey_id = sc.survey_id
where
    su.external_id = 5918 and
--     u.id = 24 and
 att.attribute_name in ('resident_ages', 'heating_control_type', 'temperature_preference', 'heating_basis', 'heating_type')
-- group by u.id, su.external_id
-- limit 100
) x
       left outer join ref_meterpoints mp on mp.account_id = x.account_id and meterpointtype = 'G'
                                     and (mp.supplyenddate is null or mp.supplyenddate >= getdate())
       inner join ref_meters m on m.account_id = mp.account_id and m.meter_point_id = mp.meter_point_id and m.removeddate is null
       inner join ref_registers reg on reg.account_id = mp.account_id and reg.meter_id = m.meter_id
       left outer join ref_tariff_history_gas_ur tf on tf.account_id = x.account_id and tf.end_date is null
       left outer join ref_cdb_quotes q on q.user_id = x.user_id
group by x.user_id,
         x.account_id
) x1
;
select count(*) from ref_cdb_survey_response
where status='declined';

select av.attribute_value from
              ref_cdb_supply_contracts su
       inner join ref_cdb_user_permissions up on su.id = up.permissionable_id and permission_level = 0
                  and permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_users u on u.id = up.user_id
       inner join ref_cdb_survey_response sr on sr.user_id = u.id
  inner join ref_cdb_surveys s on s.id = sr.survey_id
  inner join ref_cdb_survey_category sc on sc.survey_id = s.id
  inner join ref_cdb_survey_questions sq on sq.survey_category_id = sc.id
  inner join ref_cdb_attribute_types at on at.id = sq.attribute_type_id
  inner join ref_cdb_attributes a on at.id = a.attribute_type_id and
            (
             (a.entity_id = sr.user_id AND a.entity_type = 'App\\User')
               OR
             (a.entity_id = su.supply_address_id AND a.entity_type = 'App\\Address')
            )
  left outer join ref_cdb_attribute_values av on av.id = a.attribute_value_id
where attribute_name = 'heating_type'
group by av.attribute_value
order by av.attribute_value;

select * from ref_cdb_attribute_types;
select * from ref_cdb_survey_questions;


-- UDF tado_heating_summary
create or replace function tado_heating_summary(custom_values varchar(10000)) returns varchar(500)
	stable
	language plpythonu
as
$$
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

-- UDF tado_estimate_setpoint_impact
create or replace function tado_estimate_setpoint_impact(setpoint_input character varying, control_input character varying, heating_input character varying )
  returns double precision
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
$$;


-- UDF tado_estimate_heating_hours
create or replace function tado_estimate_heating_hours(heating_type_input character varying, type character varying)
  returns double precision
  stable
  language plpythonu
as $$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    heating_hours_assumption = {'working_no_kids': [12, 8],
                                'working_kids': [14, 12],
                                'retired': [16, 16],
                                'unknown': [13, 10]}

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
$$;


-- UDF tado_estimate_mean_internal_temp
create or replace function tado_estimate_mean_internal_temp(setpoint double precision, heating_hours double precision)
  returns double precision
  stable
  language plpythonu
as $$
  import logging
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  HLC= 250.0
  TIMECONSTANT= 50.0
  EXTTEMP= 7.0
  HEATCAPACITY= 0.11 * 100

  unheated_hours=24.0-heating_hours

  unheated_heat_loss = HEATCAPACITY * (setpoint - EXTTEMP) * (1.-(-unheated_hours/TIMECONSTANT))
  mit_unheated = EXTTEMP + unheated_heat_loss * 1000000 / (HLC * unheated_hours * 3600)

  mit = ((setpoint * heating_hours) + (mit_unheated * unheated_hours)) / 24.0
  return mit
$$;


