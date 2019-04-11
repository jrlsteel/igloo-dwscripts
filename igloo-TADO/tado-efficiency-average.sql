drop table ref_calculated_tado_efficiency_average;


create table ref_calculated_tado_efficiency_average
as
select avg(perc_diff)            as avg_perc_diff,
       avg(savings_in_pounds)    as avg_savings_in_pounds,
       stddev(perc_diff)         as stdev_perc_diff,
       stddev(savings_in_pounds) as stdev_savings_in_pounds,
       getdate()                    etlchange
from ref_calculated_tado_efficiency_batch
where perc_diff < 0
  and base_temp != -99
  and heating_basis != 'unknown'
  and heating_control != 'unknown'
  and family_category != 'unknown'
  and heating_source = 'gasboiler'
  and est_annual_fuel_used > 0
  and unit_rate_with_vat > 0;



