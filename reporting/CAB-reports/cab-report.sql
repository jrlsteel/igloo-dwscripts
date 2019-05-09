select
       sum(case when meter_point_type = 'E' then 1 else 0 end) as e1,
       sum(case when meter_point_type = 'E' and switch_days_wl_0 <= 21 then 1 else 0 end) as e2_within_21_days,
       sum(case when meter_point_type = 'E' and switch_days_wl_0 < 21 then 1 else 0 end) as less_than_21,
--        sum(case when switch_days_wl_0 = 21 then 1 else 0 end) as equal_to_21,
--        sum(case when switch_days_wl_1 > 21 then 1 else 0 end) as greater_than_21_asso,
--        sum(case when switch_days_wl_1 < 21 then 1 else 0 end) as less_than_21_asso,
--        sum(case when switch_days_wl_1 = 21 then 1 else 0 end) as equal_to_21_asso,
       count(*) as total from (
select
       external_id as account_id,
       mp.meterpointnumber as mpxn,
       mp.meterpointtype meter_point_type,
       su.created_at as welcome_letter_date_wl_0,
       mp.associationstartdate association_start__wl_1,
       mp.supplystartdate as supply_start_date,
       mp.supplyenddate as supply_end_date,
       datediff(days,su.created_at, mp.supplystartdate) as switch_days_wl_0,
       datediff(days,mp.associationstartdate, mp.supplystartdate) as switch_days_wl_1,
--        case when rsg.account_id is not null then 'Y' else 'N' end as gas_had_negative_status,
--        case when rse.account_id is not null then 'Y' else 'N' end as elec_had_negative_status,
       case when mp.supplyenddate is not null and mp.supplystartdate > mp.supplyenddate
              then 'Y' else 'N' end as end_before_start
from ref_cdb_supply_contracts su
      inner join ref_meterpoints mp on mp.account_id = su.external_id
where mp.supplystartdate between '2019-01-01' and '2019-03-31'
order by account_id, meterpoint_type)



