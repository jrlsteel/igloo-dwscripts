create or replace view vw_metering_report_reads_info as
select max(
         case when y.n = 1 then estimation_value else 0 end) over (partition by y.register_id) latest_eac,
       max(
         case when y.n = 2 then estimation_value else 0 end) over (partition by y.register_id) previous_eac,
       y.account_id,
       y.meterpointnumber,
       y.registerreference,
       y.register_id,
       y.no_of_digits,
       y.meterreadingdatetime,
       y.meterreadingcreateddate,
       y.meterreadingsourceuid,
       y.meterreadingtypeuid,
       y.meterreadingstatusuid,
       y.corrected_reading,
       y.total_reads
from (select r.*,
             dense_rank() over (partition by account_id, register_id order by meterreadingdatetime desc) n,
             count(*) over (partition by account_id, register_id)                                        total_reads
      from ref_readings_internal_valid r) y
       left outer join ref_estimates_elec_internal ee
         on ee.account_id = y.account_id and y.meterpointnumber = ee.mpan and
            y.registerreference = ee.register_id
              and y.meterserialnumber = ee.serial_number and
            ee.effective_from = y.meterreadingdatetime