-- create table ref_calculated_tado_efficiency_average_2019_06_25
--   as select * from ref_calculated_tado_efficiency_average;

select avg(savings_perc)            as avg_perc_diff,
       avg(savings_in_pounds)    as avg_savings_in_pounds,
       stddev(savings_perc)         as stdev_perc_diff,
       stddev(savings_in_pounds) as stdev_savings_in_pounds,
       getdate()                    etlchange
from ref_calculated_tado_efficiency_batch
where savings_perc_source = 'tado_perc'