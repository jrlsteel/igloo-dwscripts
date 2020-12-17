create view vw_ref_calculated_metering_portfolio_elec_report as

select cmpe.*
, case when cmpe.deviceid != '' then 'Y' else 'N' end dcc_enabled
, case when cmpe.meter_id in (
select distinct meter_id
from (select rsd.meter_id, count(distinct rsd.register_id) as cnt
from ref_readings_smart_daily rsd
group by rsd.meter_id) stg
where stg.cnt > 1
) then 'Y'   else 'N'  end as multi_register
, case when floor(last_read.total_consumption) != last_read.register_value then 'Y' else 'N' end total_tou_mismatch
, last_read.timestamp last_smart_read_datetime
, last_read.register_value last_smart_read_value
, last_read.total_consumption last_smart_read_total
, latest_logs.status last_smart_billing_read_status
, latest_logs.submission_timestamp last_smart_billing_read_datetime

from ref_calculated_metering_portfolio_elec_report cmpe

left join (
select *
from (select *,
row_number()
over (partition by account_id, meterpoint_id, meter_id, register_id, register_num order by etlchange desc) as rn
from ref_readings_smart_daily
)
where rn = 1 and register_num = 1
) last_read
on cmpe.account_id = last_read.account_id and cmpe.meter_point_id = last_read.meterpoint_id
and cmpe.meter_id = last_read.meter_id and cmpe.register_id = last_read.register_id

left join (
select logs.*
from (select l.*,
ROW_NUMBER()
OVER (PARTITION BY l.account_id, l.meter_point_id, l.register_id ORDER BY submission_timestamp desc) as rn
from ref_ensek_reading_submission_logs l) logs
where logs.rn = 1
) latest_logs

on cmpe.account_id = latest_logs.account_id and cmpe.meter_point_id = latest_logs.meter_point_id and cmpe.register_id = latest_logs.register_id

order by account_id, meter_id, register_id, register_num;


create view ref_calculated_metering_portfolio_gas_report as

select cmpe.*
, case when cmpe.device_id != '' then 'Y' else 'N' end dcc_enabled
, 'N' as multi_register
, case when floor(last_read.total_consumption) != last_read.register_value then 'Y' else 'N' end total_tou_mismatch
, last_read.timestamp last_smart_read_datetime
, last_read.register_value last_smart_read_value
, last_read.total_consumption last_smart_read_total
, latest_logs.status last_smart_billing_read_status, latest_logs.submission_timestamp last_smart_billing_read_datetime

from ref_calculated_metering_portfolio_gas_report cmpe

left join (
select *
from (select *,
row_number()
over (partition by account_id, meterpoint_id, meter_id, register_id, register_num order by etlchange desc) as rn
from ref_readings_smart_daily
)
where rn = 1 and register_num = 1
) last_read
on cmpe.account_id = last_read.account_id and cmpe.meter_point_id = last_read.meterpoint_id
and cmpe.meter_id = last_read.meter_id and cmpe.register_id = last_read.register_id

left join (
select logs.*
from (select l.*,
ROW_NUMBER()
OVER (PARTITION BY l.account_id, l.meter_point_id, l.register_id ORDER BY submission_timestamp desc) as rn
from ref_ensek_reading_submission_logs l) logs
where logs.rn = 1
) latest_logs

on cmpe.account_id = latest_logs.account_id and cmpe.meter_point_id = latest_logs.meter_point_id and cmpe.register_id = latest_logs.register_id

order by account_id, meter_id, register_id, register_num;


