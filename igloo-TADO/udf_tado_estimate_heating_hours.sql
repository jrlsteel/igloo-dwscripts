-- UDF tado_estimate_heating_hours
create or replace function tado_estimate_heating_hours(heating_type_input character varying, type character varying)
  returns double precision
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
$$;

