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
