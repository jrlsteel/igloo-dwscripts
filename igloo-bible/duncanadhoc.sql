select su.external_id                       as account_id,
       rcup.user_id                         as igloo_user_id,
       rca.postcode                         as supply_postcode,
       ac.status                            as account_status,
       mp_elec.meterpointtype               as meterpoint_type_elec,
       mp_gas.meterpointtype                as meterpoint_type_gas,
       case
         when (mp_elec.meterpointtype is not null and mp_gas.meterpointtype is not null)
                 then 'dual_fuel'
         else 'single_fuel' end             as dual,
       case
         when (mp_elec.supplyenddate is null or mp_elec.supplyenddate >= current_date) then 'Live'
         else 'Not Live' end                as meterpoints_status_elec,
       case
         when (
           mp_gas.meterpointtype = 'G' and (mp_gas.supplyenddate is null or mp_gas.supplyenddate >= current_date))
                 then 'Live'
         else 'Not Live' end                as meterpoints_status_gas,
       rse.status                           as registration_status_elec,
       rsg.status                           as registration_status_gas,
       mp_elec.supplystartdate              as supply_startdate_elec,
       mp_elec.supplyenddate                as supply_enddate_elec,
       mp_gas.supplystartdate               as supply_startdate_gas,
       mp_gas.supplyenddate                 as supply_enddate_gas,
       su.created_at                        as supply_contract_creation_date,
       mp_elec.meter_point_id               as meter_point_id_elec,
       mp_gas.meter_point_id                as meter_point_id_gas,
       mt_elec.meter_id                     as meter_id_elec,
       mt_gas.meter_id                      as meter_id_gas,
       reg_elec.register_id                 as register_id_elec,
       reg_gas.register_id                  as register_id_gas,
       reg_elec.registers_eacaq             as register_elec_eacaq,
       reg_gas.registers_eacaq              as register_gas_eacaq,
       reg_elec.registers_registerreference as register_elec_reference,
       reg_gas.registers_registerreference  as register_gas_reference,
       reg_elec.registers_tpr               as register_elec_tpr,
       reg_gas.registers_tpr                as register_gas_tpr
from ref_cdb_supply_contracts su --inner join temp_cab_dates_quarter4_2018 we on we.external_id = su.external_id
       inner join ref_cdb_user_permissions rcup on su.id = rcup.permissionable_id and permission_level = 0 and
                                                   permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_users rcu on rcup.user_id = rcu.id
       inner join ref_cdb_addresses rca on su.supply_address_id = rca.id
       left outer join ref_account_status ac on ac.account_id = su.external_id
       left outer join ref_meterpoints mp_elec on mp_elec.account_id = su.external_id and mp_elec.meterpointtype = 'E'
       left outer join ref_meterpoints_attributes mpa_elec on mp_elec.meter_point_id = mpa_elec.meter_point_id and
                                                              attributes_effectivetodate is null
       left outer join ref_meters mt_elec
         on mp_elec.meter_point_id = mt_elec.meter_point_id and mt_elec.removeddate is null
       left outer join ref_meters_attributes mta_elec on mt_elec.meter_id = mta_elec.meter_id
       left outer join ref_registers reg_elec on mt_elec.meter_id = reg_elec.meter_id
       left outer join ref_registers_attributes rga_elec on reg_elec.register_id = rga_elec.register_id
       left outer join ref_meterpoints mp_gas on mp_gas.account_id = su.external_id and mp_gas.meterpointtype = 'G'
       left outer join ref_meters mt_gas on mp_gas.meter_point_id = mt_gas.meter_point_id and mt_gas.removeddate is null
       left outer join ref_registers reg_gas on mt_gas.meter_id = reg_gas.meter_id
       left outer join ref_registers_attributes rga_gas on reg_gas.register_id = rga_gas.register_id
       left outer join ref_registrations_status_gas rsg on mp_elec.account_id = rsg.account_id
       left outer join ref_registrations_status_elec rse on mp_elec.account_id = rse.account_id

group by su.external_id,
         rcup.user_id,
         rca.postcode,
         ac.status,
         mp_elec.meterpointtype,
         mp_gas.meterpointtype,
         case
           when (mp_elec.meterpointtype is not null and mp_gas.meterpointtype is not null)
                   then 'dual_fuel'
           else 'single_fuel' end,
         case
           when (mp_elec.meterpointtype = 'E' and (mp_elec.supplyenddate is null or
                                                   (mp_elec.supplyenddate >= current_date and
                                                    mp_elec.supplyenddate >= mp_elec.supplystartdate))) then 'Live'
           else 'Not Live' end,
         case
           when (mp_gas.meterpointtype = 'G' and (mp_gas.supplyenddate is null or
                                                  (mp_gas.supplyenddate >= current_date and
                                                   mp_gas.supplyenddate >= mp_gas.supplystartdate))) then 'Live'
           else 'Not Live' end,
         rse.status,
         rsg.status,
         mp_elec.supplystartdate,
         mp_elec.supplyenddate,
         mp_gas.supplystartdate,
         mp_gas.supplyenddate,
         su.created_at,
         mp_elec.meter_point_id,
         mp_gas.meter_point_id,
         mt_elec.meter_id,
         mt_gas.meter_id,
         reg_elec.register_id,
         reg_gas.register_id,
         reg_elec.registers_eacaq,
         reg_gas.registers_eacaq,
         reg_elec.registers_registerreference,
         reg_gas.registers_registerreference,
         reg_elec.registers_tpr,
         reg_gas.registers_tpr
    --MeterPoints Attributes Elec
order by su.external_id
