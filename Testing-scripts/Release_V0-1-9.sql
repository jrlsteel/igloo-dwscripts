-- 1) return all live accounts that do not exist in consumption accuracy
select rm.account_id,
       greatest(rm.supplystartdate, rm.associationstartdate) as ssd,
       least(rm.supplyenddate, rm.associationenddate)        as sed,
       rm.meter_point_id,
       met.meter_id,
       reg.register_id,
       meterpointtype,
       sc.source
from vw_supply_contracts_with_occ_accs sc
         inner join ref_meterpoints rm on rm.account_id = sc.external_id
         left join ref_meters met on rm.account_id = met.account_id and met.meter_point_id = rm.meter_point_id and
                                     met.removeddate is null
         left join ref_registers reg on reg.account_id = met.account_id and reg.meter_id = met.meter_id
         left join ref_consumption_accuracy_elec ca_elec
                   on ca_elec.account_id = rm.account_id and rm.meterpointtype = 'E'
         left join ref_consumption_accuracy_gas ca_gas on ca_gas.account_id = rm.account_id and rm.meterpointtype = 'G'
where greatest(rm.associationstartdate, rm.supplystartdate) <= getdate()
  and (least(rm.associationenddate, rm.supplyenddate) is null or
       least(rm.associationenddate, rm.supplyenddate) >= getdate())
  and ((ca_elec.account_id is null and rm.meterpointtype = 'E') or
       (ca_gas.account_id is null and rm.meterpointtype = 'G'))
and register_id is not null
order by account_id;

-- 2.1) return accounts where an aq was calculated before but not now
select *
from ref_calculated_igl_ind_aq_backup_R019 aq_backup
         left join ref_calculated_igl_ind_aq aq
                   on aq.account_id = aq_backup.account_id and aq.register_id = aq_backup.register_id
where aq.account_id is null;
-- 2.2)
select aq_backup.*, mp.meter_point_id, met.meter_id, met.removeddate, reg.register_id
from ref_calculated_aq_backup_R019 aq_backup
         inner join ref_meterpoints mp on mp.account_id = aq_backup.account_id and
                                          (least(mp.supplyenddate, mp.associationenddate) is null or
                                           least(mp.supplyenddate, mp.associationenddate) >= getdate())
         inner join ref_meters met on met.account_id = mp.account_id and met.meter_point_id = mp.meter_point_id and
                                      met.removeddate is null
         inner join ref_registers reg on reg.account_id = met.account_id and reg.meter_id = met.meter_id and
                                         reg.register_id = aq_backup.register_id
         left join ref_calculated_aq aq
                   on aq.account_id = aq_backup.account_id and aq.register_id = aq_backup.register_id
where aq.account_id is null
order by aq_backup.account_id;

-- 3) number of accounts in cons_acc with differing elec/gas latest_reading_datetimes
select case
           when elec.reading_datetime > gas.reading_datetime then 'Elec_Most_Recent'
           when elec.reading_datetime < gas.reading_datetime then 'Gas_Most_Recent'
           else 'Equal' end as read_date_comparison,
       count(*)
from ref_consumption_accuracy_gas gas
         inner join ref_consumption_accuracy_elec elec on gas.account_id = elec.account_id
group by read_date_comparison;

-- 4) check for duplicated details being properly handled in consumption accuracy
select *
from ref_consumption_accuracy_elec
where account_id = 58955;
select *
from ref_consumption_accuracy_gas
where account_id = 58955;

-- 5) number of each update type in audit table
select etlchange, etlchangetype, count(*) as num_updates
from ref_consumption_accuracy_elec_audit
group by etlchange, etlchangetype
order by etlchange desc, etlchangetype;
select etlchange, etlchangetype, count(*) as num_updates
from ref_consumption_accuracy_gas_audit
group by etlchange, etlchangetype
order by etlchange desc, etlchangetype;

-- 6) closed accounts with values in cons acc
select cf.account_id, cf.elec_ed
from vw_customer_file cf
         left join ref_consumption_accuracy_elec ca_elec on cf.account_id = ca_elec.account_id
where ca_elec.account_id is not null
  and cf.elec_reg_status is not null
  and cf.elec_reg_status = 'final'
select cf.account_id, cf.gas_ed
from vw_customer_file cf
         left join ref_consumption_accuracy_gas ca_gas on cf.account_id = ca_gas.account_id
where ca_gas.account_id is not null
  and cf.gas_reg_status is not null
  and cf.gas_reg_status = 'final'


-- 7) closed accounts (which were active after cons acc was released) that are not present in cons acc closed views
select cf.account_id, cf.elec_ed
from vw_customer_file cf
         left join vw_consumption_accuracy_elec_closed ca_elec on cf.account_id = ca_elec.account_id
where ca_elec.account_id is null
  and cf.elec_reg_status is not null
  and cf.elec_reg_status = 'final'
  and elec_ed > '2019-07-09';
select cf.account_id, cf.gas_ed
from vw_customer_file cf
         left join vw_consumption_accuracy_gas_closed ca_gas on cf.account_id = ca_gas.account_id
where ca_gas.account_id is null
  and cf.gas_reg_status is not null
  and cf.gas_reg_status = 'final'
  and gas_ed > '2019-07-09';

-- 8) number of each type of read in rriv
select rriv_new.*, old_count
from (select meterreadingsourceuid, meterpointtype, count(*) as new_count
      from ref_readings_internal_valid
      group by meterreadingsourceuid, meterpointtype) rriv_new
         full join
     (select meterreadingsourceuid, meterpointtype, count(*) as old_count
      from ref_readings_internal_valid_backup_R019
      group by meterreadingsourceuid, meterpointtype) rriv_old
     on rriv_new.meterreadingsourceuid = rriv_old.meterreadingsourceuid and
        rriv_new.meterpointtype = rriv_old.meterpointtype;

