-- Weekly run
select account_id, case when count(mp_sed) < count(*) then null else max(mp_sed) end as sed
from (select coalesce(account_id, external_id) as account_id, least(supplyenddate, associationenddate) as mp_sed
      from ref_meterpoints_raw mpr
               full join ref_cdb_supply_contracts sc on mpr.account_id = sc.external_id) mps
group by account_id
having sed is null
    or datediff(years, sed, getdate()) < 1
order by account_id

-- Daily run
select account_id, case when count(mp_sed) < count(*) then null else max(mp_sed) end as sed
from (select account_id, least(supplyenddate, associationenddate) as mp_sed
      from ref_meterpoints_raw mpr) mps
group by account_id
having sed is null
    or datediff(weeks, sed, getdate()) < 8
order by account_id


-- Acc_MP_ID
select distinct account_id, meter_point_id, meterpointnumber, meterpointtype
from ref_meterpoints_raw
where datediff(weeks, greatest(supplystartdate, associationstartdate), getdate()) < 1
order by account_id