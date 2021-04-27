with cte_data as (
    select reads.meter_reading_id,
           reads.meterreadingdatetime,
           reads.meterreadingtypeuid,
           reads.meterreadingsourceuid,
           reads.meterreadingcreateddate,
           reads.meterreadingstatusuid,
           reads.meterpointnumber,
           reads.meterserialnumber,
           reads.readingvalue,
           reads.registerreference
    from ref_readings_internal reads
    where meterreadingdatetime >= date_trunc('year', CURRENT_DATE) - INTERVAL '1 year'
    and meterreadingsourceuid = 'DC'
    and meterreadingstatusuid = 'VALID'
)
select count(*) over() as count,
       sum(readingvalue) over() as last_numerical_sum,
       data.*
from cte_data data