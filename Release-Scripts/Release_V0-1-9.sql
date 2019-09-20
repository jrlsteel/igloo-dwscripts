-- remove backups
drop table ref_consumption_accuracy_elec_backup_R019;
drop table ref_consumption_accuracy_gas_backup_R019;
drop table ref_consumption_accuracy_elec_audit_backup_R019;
drop table ref_consumption_accuracy_gas_audit_backup_R019;
drop table ref_calculated_igl_ind_aq_backup_R019;
drop table ref_calculated_aq_backup_R019;
drop table ref_readings_internal_valid_backup_R019;


-- DWH TABLE BACKUPS
create table ref_consumption_accuracy_elec_backup_R019 as
select *
from ref_consumption_accuracy_elec;
create table ref_consumption_accuracy_gas_backup_R019 as
select *
from ref_consumption_accuracy_gas;
create table ref_consumption_accuracy_elec_audit_backup_R019 as
select *
from ref_consumption_accuracy_elec_audit;
create table ref_consumption_accuracy_gas_audit_backup_R019 as
select *
from ref_consumption_accuracy_gas_audit;
create table ref_calculated_igl_ind_aq_backup_R019 as
select *
from ref_calculated_igl_ind_aq;
create table ref_calculated_aq_backup_R019 as
select *
from ref_calculated_aq;
create table ref_readings_internal_valid_backup_R019 as
select *
from ref_readings_internal_valid;


-- TRUNCATE TABLES
truncate table ref_calculated_aq;
truncate table ref_calculated_igl_ind_aq;
truncate table ref_readings_internal_valid;


--select max(etlchange) from "igloosense-preprod".public.ref_consumption_accuracy_gas
-- revert tables to backups if necessary
-- truncate table ref_calculated_aq;
-- truncate table ref_calculated_igl_ind_aq;
-- truncate table ref_readings_internal_valid;
-- insert into ref_calculated_aq
-- select *
-- from ref_calculated_aq_backup_R019;
-- insert into ref_calculated_igl_ind_aq
-- select *
-- from ref_calculated_igl_ind_aq_backup_R019;
-- insert into ref_readings_internal_valid
-- select *
-- from ref_readings_internal_valid_backup_R019;
-- truncate table ref_consumption_accuracy_elec;
-- truncate table ref_consumption_accuracy_gas;
-- truncate table ref_consumption_accuracy_elec_audit;
-- truncate table ref_consumption_accuracy_gas_audit;
-- insert into ref_consumption_accuracy_elec
-- select *
-- from ref_consumption_accuracy_elec_backup_R019;
-- insert into ref_consumption_accuracy_gas
-- select *
-- from ref_consumption_accuracy_gas_backup_R019;
-- insert into ref_consumption_accuracy_elec_audit
-- select *
-- from ref_consumption_accuracy_elec_audit_backup_R019;
-- insert into ref_consumption_accuracy_gas_audit
-- select *
-- from ref_consumption_accuracy_gas_audit_backup_R019;

-- NEW VIEWS
-- 1
create or replace view vw_consumption_accuracy_elec_closed as
select account_id,
       reading_datetime,
       pa_cons_elec,
       igl_ind_eac,
       ind_eac,
       quotes_eac,
       etlchange
from (select *, row_number() over (partition by account_id order by etlchange desc) as recency
      from ref_consumption_accuracy_elec_audit
     ) last_update
where last_update.recency = 1
  and etlchangetype = 'r'
order by account_id;

-- 2
create or replace view vw_consumption_accuracy_gas_closed as
select account_id,
       reading_datetime,
       pa_cons_gas,
       igl_ind_aq,
       ind_aq,
       quotes_aq,
       etlchange
from (select *, row_number() over (partition by account_id order by etlchange desc) as recency
      from ref_consumption_accuracy_gas_audit
     ) last_update
where last_update.recency = 1
  and etlchangetype = 'r'
order by account_id;

-- 3
create or replace view vw_supply_contracts_with_occ_accs as
    select id,
           supply_address_id,
           registration_id,
           external_id,
           external_uuid,
           status,
           created_at,
           updated_at,
           'sc' as source
    from ref_cdb_supply_contracts
    union
    (select -1                                    as id,
            max(mp_address_ids.supply_address_id) as supply_address_id,
            -1                                    as registration_id,
            account_id                            as external_id,
            '-1'                                  as external_uuid,
            null                                  as status,
            null                                  as created_at,
            null                                  as updated_at,
            'mpr'                                 as source
     from ref_meterpoints_raw mpr
              left join
          (select meterpointnumber, max(sc.supply_address_id) as supply_address_id
           from ref_meterpoints_raw mpr
                    left join ref_cdb_supply_contracts sc on mpr.account_id = sc.external_id
           group by meterpointnumber) mp_address_ids
          on mp_address_ids.meterpointnumber = mpr.meterpointnumber
              left join ref_cdb_supply_contracts sc on mpr.account_id = sc.external_id
     where sc.external_id is null
     group by mpr.account_id);