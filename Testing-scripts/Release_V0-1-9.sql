-- 1) return all live accounts that do not exist in consumption accuracy
select rm.account_id,
       greatest(rm.supplystartdate, rm.associationstartdate) as ssd,
       least(rm.supplyenddate, rm.associationenddate)        as sed,
       meter_point_id,
       meterpointtype,
       sc.source
from vw_supply_contracts_with_occ_accs sc
         inner join ref_meterpoints rm on rm.account_id = sc.external_id
         left join ref_consumption_accuracy_elec ca_elec
                   on ca_elec.account_id = rm.account_id and rm.meterpointtype = 'E'
         left join ref_consumption_accuracy_gas ca_gas on ca_gas.account_id = rm.account_id and rm.meterpointtype = 'G'
where greatest(rm.associationstartdate, rm.supplystartdate) <= getdate()
  and (least(rm.associationenddate, rm.supplyenddate) is null or
       least(rm.associationenddate, rm.supplyenddate) >= getdate())
  and ((ca_elec.account_id is null and rm.meterpointtype = 'E') or
       (ca_gas.account_id is null and rm.meterpointtype = 'G'));


-- 2) return accounts where an aq was calculated before but not now
select *
from ref_calculated_igl_ind_aq_backup_V019 aq_backup
         left join ref_calculated_igl_ind_aq aq
                   on aq.account_id = aq_backup.account_id and aq.register_id = aq_backup.register_id
where aq.account_id is null;
select *
from ref_calculated_aq_backup_V019 aq_backup
         left join ref_calculated_aq aq
                   on aq.account_id = aq_backup.account_id and aq.register_id = aq_backup.register_id
where aq.account_id is null;

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
      from ref_readings_internal_valid_backup_V019
      group by meterreadingsourceuid, meterpointtype) rriv_old
     on rriv_new.meterreadingsourceuid = rriv_old.meterreadingsourceuid and
        rriv_new.meterpointtype = rriv_old.meterpointtype;