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

