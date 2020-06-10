-- DMRE-408
-- Report Name Energy Switch Guarantee: Total Switches


-- 24-04-2019
-- 26-04-2019 Added  datediff(days,association_start_wl_1, wl0_transposed_date) as new_switch_days_asw11_and_w10td

select ens.account_id,
--        '' as ET,
       ens.mpxn,
       ens.meterpoint_type,
       ens.welcome_letter_date_wl_0,
       ens.day_of_week,
       ens.wl0_transposed_date,
       ens.association_start_wl_1,
       ens.supply_start_date,
       ens.diff_btw_wl_0_wl_1,
       case when ens.diff_btw_wl_0_wl_1 > 1 then 'Y' else 'N' end as delay,
       datediff(days,wl0_transposed_date, supply_start_date) as switch_days_ssd_wl_0_trans,
       ens.gas_had_negative_status,
       ens.elec_had_negative_status,
       ens.end_before_start
--        datediff(days,wl0_transposed_date, association_start_wl_1) as diff_btw_wl_1_wl_0_trans


--        sum(case when switch_days_wl_0 > 21 then 1 else 0 end) as greater_than_21,
--        sum(case when switch_days_wl_0 < 21 then 1 else 0 end) as less_than_21,
--        sum(case when switch_days_wl_0 = 21 then 1 else 0 end) as equal_to_21,
--        sum(case when switch_days_wl_1 > 21 then 1 else 0 end) as greater_than_21_asso,
--        sum(case when switch_days_wl_1 < 21 then 1 else 0 end) as less_than_21_asso,
--        sum(case when switch_days_wl_1 = 21 then 1 else 0 end) as equal_to_21_asso,
--        count(*) as total
from (
select
       external_id as account_id,
       mp.meterpointnumber as mpxn,
       mp.meterpointtype meterpoint_type,
       su.created_at as welcome_letter_date_wl_0,
       date_part(dow, su.created_at) as day_of_week,
       case when (date_part(dow, su.created_at) = 5 and date_part(hours, su.created_at) >= 17 and date_part(minutes, su.created_at) > 30)
                  or date_part(dow, su.created_at) in (6,0)
                then 'non-working-days'
           else
                'working-days' end
        as week_days,
        case when (date_part(dow, su.created_at) = 5 and date_part(hours, su.created_at) >= 17 and date_part(minutes, su.created_at) > 30)
                  or date_part(dow, su.created_at) = 6
                then dateadd(days,(8 - date_part(dow, su.created_at))::int , su.created_at)
            else case when date_part(dow, su.created_at) = 0 then dateadd(days, 1, su.created_at)
           else
                su.created_at end end
        as wl0_transposed_date,
       mp.associationstartdate association_start_wl_1,
       mp.supplystartdate as supply_start_date,
--        mp.supplyenddate as supply_end_date,
       datediff(days,su.created_at, mp.associationstartdate) as diff_btw_wl_0_wl_1,
       datediff(days,su.created_at, mp.supplystartdate) as switch_days_wl_0,
       datediff(days,mp.associationstartdate, mp.supplystartdate) as switch_days_wl_1,
       case when rsmg.account_id is not null then 'Y' else 'N' end as gas_had_negative_status,
       case when rsme.account_id is not null then 'Y' else 'N' end as elec_had_negative_status,
       case when mp.supplyenddate is not null and mp.supplystartdate > mp.supplyenddate
              then 'Y' else 'N' end as end_before_start

from ref_cdb_supply_contracts su
      inner join ref_meterpoints_raw mp on mp.account_id = su.external_id
--       left outer join (select account_id from ref_registrations_status_gas_audit
--                         where status in
--                           ('Tracker.Registration.Gas.Objection.Upheld',
--                            'Tracker.Registration.Gas.Objection.Received',
--                            'Tracker.Registration.Gas.Objection.Lifted',
--                            'Tracker.Registration.Gas.Registration.Rejected',
--                            'Tracker.Registration.Gas.Abandoned',
--                            'Tracker.Registration.Gas.Cancelled.in.Cooling.Off')
--                         group by account_id) rsg on rsg.account_id = mp.account_id
--       left outer join (select account_id from ref_registrations_status_elec_audit
--                         where status in
--                           ('Tracker.Registration.Objection.Upheld',
--                             'Tracker.Registration.Objection.Received',
--                            'Tracker.Registration.Objection.Lifted',
--                            'Tracker.Registration.Registration.Rejected',
--                            'Tracker.Registration.Withdrawn',
--                            'Tracker.Registration.Cancelled.In.Cooling.Off')
--                         group by account_id) rse on rse.account_id = mp.account_id
      left outer join (select account_id from ref_registrations_meterpoints_status_gas_audit
                        where status in
                          ('Tracker.Registration.Gas.Objection.Upheld',
                           'Tracker.Registration.Gas.Objection.Received',
                           'Tracker.Registration.Gas.Objection.Lifted',
                           'Tracker.Registration.Gas.Registration.Rejected',
                           'Tracker.Registration.Gas.Abandoned',
                           'Tracker.Registration.Gas.Cancelled.in.Cooling.Off')
                        group by account_id) rsmg on rsmg.account_id = mp.account_id
      left outer join (select account_id from ref_registrations_meterpoints_status_elec_audit
                        where status in
                          ('Tracker.Registration.Objection.Upheld',
                            'Tracker.Registration.Objection.Received',
                           'Tracker.Registration.Objection.Lifted',
                           'Tracker.Registration.Registration.Rejected',
                           'Tracker.Registration.Withdrawn',
                           'Tracker.Registration.Cancelled.In.Cooling.Off')
                        group by account_id) rsme on rsme.account_id = mp.account_id
where mp.supplystartdate between '2019-10-01' and '2019-12-31'
order by account_id, meterpoint_type
) ens
-- where week_days = 'non-working-days' and day_of_week in(5,6,0)
;

-- # Date: 18-04-2019
select
       sum(case when switch_days_wl_0 > 21 then 1 else 0 end) as greater_than_21,
       sum(case when switch_days_wl_0 < 21 then 1 else 0 end) as less_than_21,
       sum(case when switch_days_wl_0 = 21 then 1 else 0 end) as equal_to_21,
       sum(case when switch_days_wl_1 > 21 then 1 else 0 end) as greater_than_21_asso,
       sum(case when switch_days_wl_1 < 21 then 1 else 0 end) as less_than_21_asso,
       sum(case when switch_days_wl_1 = 21 then 1 else 0 end) as equal_to_21_asso,
       count(*) as total from (
select
       external_id as ensek_id,
       mp.meterpointnumber as mpxn,
       mp.meterpointtype meterpoint_type,
       su.created_at as welcome_letter_date_wl_0,
       mp.associationstartdate association_start__wl_1,
       mp.supplystartdate as supply_start_date,
       mp.supplyenddate as supply_end_date,
       datediff(days,su.created_at, mp.supplystartdate) as switch_days_wl_0,
       datediff(days,mp.associationstartdate, mp.supplystartdate) as switch_days_wl_1,
       case when rsg.account_id is not null then 'Y' else 'N' end as gas_had_negative_status,
       case when rse.account_id is not null then 'Y' else 'N' end as elec_had_negative_status,
       case when mp.supplyenddate is not null and mp.supplystartdate > mp.supplyenddate
              then 'Y' else 'N' end as end_before_start

from ref_cdb_supply_contracts su
      inner join ref_meterpoints mp on mp.account_id = su.external_id
      left outer join (select account_id from ref_registrations_status_gas_audit
                        where status in
                          ('Tracker.Registration.Gas.Objection.Upheld',
                           'Tracker.Registration.Gas.Objection.Received',
                           'Tracker.Registration.Gas.Objection.Lifted',
                           'Tracker.Registration.Gas.Registration.Rejected',
                           'Tracker.Registration.Gas.Abandoned',
                           'Tracker.Registration.Gas.Cancelled.in.Cooling.Off')
                        group by account_id) rsg on rsg.account_id = mp.account_id
      left outer join (select account_id from ref_registrations_status_elec_audit rse
                        where status in
                          ('Tracker.Registration.Objection.Upheld',
                            'Tracker.Registration.Objection.Received',
                           'Tracker.Registration.Objection.Lifted',
                           'Tracker.Registration.Registration.Rejected',
                           'Tracker.Registration.Withdrawn',
                           'Tracker.Registration.Cancelled.In.Cooling.Off')
                        group by account_id) rse on rse.account_id = mp.account_id
where mp.supplystartdate between '2019-10-01' and '2019-12-31'
order by ensek_id, meterpoint_type) ens
-- where switch_days_createdat < 21
;

-- gas_status
-- Tracker.Registration.Gas.Objection.Received
-- Tracker.Registration.Gas.Objection.Lifted
-- Tracker.Registration.Gas.Registration.Rejected
-- Tracker.Registration.Gas.Abandoned
-- Tracker.Registration.Gas.Cancelled.in.Cooling.Off
-- Tracker.Registration.Gas.Objection.Upheld
-- Tracker.Registration.Gas.Registration.Requested
-- Tracker.Registration.Gas.Live
-- Tracker.Registration.Gas.Registration.Accepted
-- Tracker.Registration.Gas.Quote.Accepted
--
--
-- elec_status
-- Tracker.Registration.Objection.Received
-- Tracker.Registration.Objection.Lifted
-- Tracker.Registration.Registration.Rejected
-- Tracker.Registration.Withdrawn
-- Tracker.Registration.Cancelled.In.Cooling.Off
-- Tracker.Registration.Live
-- Tracker.Registration.Objection.Upheld
-- Tracker.Registration.Registration.Accepted
-- Tracker.Registration.Quote.Accepted
-- Tracker.Registration.Registration.Requested
-- Tracker.Registration.Reads.Obtained
-- Tracker.Registration.No.Reads

select * from (
select account_id,
greatest(supplystartdate, associationstartdate) as startdate,
least(supplyenddate, associationenddate) as enddate
-- supplystartdate,
-- supplyenddate,
-- associationstartdate,
-- associationenddate
from ref_meterpoints
order by account_id) x
where
x.enddate between '2019-04-01' and '2019-06-30'
and x.enddate > x.startdate
;

select * from ref_meterpoints where supplyenddate is not null;

select user_name, db_name, pid, sv.* query
from stv_recents sv
where status = 'Running';