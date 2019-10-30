create function tado_savings_segmentation(mmh_tado_status character varying, fuel_type character varying, heating_control character varying, heating_source character varying, pounds_savings double precision)
  returns integer
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
        if (heating_source == 'gasboiler' and fuel_type == 'E') or heating_source == 'oilboiler':
            if pounds_savings < 0:
                segment_id = 9
            if pounds_savings >= 0:
                segment_id = 10

        if heating_source == 'gasboiler' and fuel_type in ('B', 'G'):
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
$$;