SELECT rcte.user_id,
       rcte.account_id,
       ref_cdb_registrations.marketing                      as marketing_optin,
       ac.status                                            as account_status,
       mp_elec.meterpointtype                               as meterpoint_type_elec,
       mp_gas.meterpointtype                                as meterpoint_type_gas,
       case
         when (mp_elec.meterpointtype is not null and mp_gas.meterpointtype is not null)
                 then 'dual_fuel'
         else 'single_fuel' end                             as dual,
       case
         when (mp_elec.supplyenddate is null or mp_elec.supplyenddate >= current_date) then 'Live'
         else 'Not Live' end                                as meterpoints_status_elec,
       case
         when (
           mp_gas.meterpointtype = 'G' and (mp_gas.supplyenddate is null or mp_gas.supplyenddate >= current_date))
                 then 'Live'
         else 'Not Live' end                                as meterpoints_status_gas,
       rcte.base_temp,
       rcte.heating_basis,
       rcte.heating_control,
       rcte.fuel_type,
       rcte.ages,
       rcte.status,
       rcte.family_category,
       rcte.base_temp_used,
       rcte.estimated_temp,
       rcte.base_hours,
       rcte.estimated_hours,
       rcte.base_mit,
       rcte.estimated_mit,
       rcte.aq,
       rcte.gas_usage,
       rcte.est_annual_fuel_used,
       rcte.unit_rate_with_vat,
       rcte.amount_over_year,
       rcte.perc_diff,
       rcte.savings_in_pounds,
       rcte.segment,
       rcte.etlchange,
       rca.postcode                                         as supply_postcode,
       rse.status                                           as registration_status_elec,
       rsg.status                                           as registration_status_gas,
       mp_elec.supplystartdate                              as supply_startdate_elec,
       mp_elec.supplyenddate                                as supply_enddate_elec,
       mp_gas.supplystartdate                               as supply_startdate_gas,
       mp_gas.supplyenddate                                 as supply_enddate_gas,
       su.created_at                                        as supply_contract_creation_date,
       mp_elec.meter_point_id                               as meter_point_id_elec,
       mp_elec.meterpointnumber                             as meterpointnumber_elec,
       mp_gas.meter_point_id                                as meter_point_id_gas,
       mp_gas.meterpointnumber                              as meterpointnumber_gas,
       mt_elec.meter_id                                     as meter_id_elec,
       mt_gas.meter_id                                      as meter_id_gas,
       reg_elec.register_id                                 as register_id_elec,
       reg_gas.register_id                                  as register_id_gas,
       reg_elec.registers_eacaq                             as register_elec_eacaq,
       reg_gas.registers_eacaq                              as register_gas_eacaq,
       reg_elec.registers_registerreference                 as register_elec_reference,
       reg_gas.registers_registerreference                  as register_gas_reference,
       reg_elec.registers_tpr                               as register_elec_tpr,
       reg_gas.registers_tpr                                as register_gas_tpr
FROM ref_calculated_tado_efficiency_batch rcte
       inner join ref_cdb_supply_contracts su on rcte.account_id = su.external_id --inner join temp_cab_dates_quarter4_2018 we on we.external_id = su.external_id
       inner join ref_cdb_registrations on su.registration_id = ref_cdb_registrations.id
       inner join ref_cdb_user_permissions rcup
         on su.id = rcup.permissionable_id and permission_level = 0 and permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_users rcu on rcup.user_id = rcu.id
       inner join ref_cdb_addresses rca on su.supply_address_id = rca.id --Ensek Meterpoint Elec
       left outer join ref_meterpoints mp_elec on mp_elec.account_id = su.external_id and mp_elec.meterpointtype = 'E'
       left outer join ref_meterpoints_attributes mpa_elec
         on mp_elec.account_id = mpa_elec.account_id and mp_elec.meter_point_id = mpa_elec.meter_point_id and
            attributes_effectivetodate is null
       left outer join ref_meters mt_elec
         on mp_elec.account_id = mt_elec.account_id and mp_elec.meter_point_id = mt_elec.meter_point_id and
            mt_elec.removeddate is null --left outer join ref_meters_attributes mta_elec on mt_elec.meter_id = mta_elec.meter_id
       left outer join ref_registers reg_elec
         on mt_elec.account_id = reg_elec.account_id and mt_elec.meter_id = reg_elec.meter_id --Ensek Meterpoint Gas
       left outer join ref_meterpoints mp_gas on mp_gas.account_id = su.external_id and mp_gas.meterpointtype = 'G'
       left outer join ref_meterpoints_attributes mpa_gas
         on mp_gas.account_id = mpa_gas.account_id and mp_gas.meter_point_id = mpa_gas.meter_point_id and
            mpa_gas.attributes_effectivetodate is null
       left outer join ref_meters mt_gas
         on mp_gas.account_id = mt_gas.account_id and mp_gas.meter_point_id = mt_gas.meter_point_id and
            mt_gas.removeddate is null
       left outer join ref_registers reg_gas on mt_gas.account_id = reg_gas.account_id and
                                                mt_gas.meter_id = reg_gas.meter_id --left outer join ref_registers_attributes rga_elec on reg_elec.register_id = rga_elec.register_id
         --Status
       left outer join ref_account_status ac on ac.account_id = su.external_id
       left outer join ref_registrations_meterpoints_status_gas rsg
         on mp_gas.account_id = rsg.account_id and mp_gas.meter_point_id = rsg.meterpoint_id
       left outer join ref_registrations_meterpoints_status_elec rse
         on mp_elec.account_id = rse.account_id and mp_elec.meter_point_id = rse.meterpoint_id
group by
          rcte.user_id,
       rcte.account_id,
       ref_cdb_registrations.marketing,
       rcte.base_temp,
       rcte.heating_basis,
       rcte.heating_control,
       rcte.fuel_type,
       rcte.ages,
       rcte.status,
       rcte.family_category,
       rcte.base_temp_used,
       rcte.estimated_temp,
       rcte.base_hours,
       rcte.estimated_hours,
       rcte.base_mit,
       rcte.estimated_mit,
       rcte.aq,
       rcte.gas_usage,
       rcte.est_annual_fuel_used,
       rcte.unit_rate_with_vat,
       rcte.amount_over_year,
       rcte.perc_diff,
       rcte.savings_in_pounds,
       rcte.segment,
       rcte.etlchange,
       rca.postcode                                     ,
       ac.status                                        ,
       mp_elec.meterpointtype                           ,
       mp_gas.meterpointtype                            ,
       case
         when (mp_elec.meterpointtype is not null and mp_gas.meterpointtype is not null)
                 then 'dual_fuel'
         else 'single_fuel' end                         ,
       case
         when (mp_elec.supplyenddate is null or mp_elec.supplyenddate >= current_date) then 'Live'
         else 'Not Live' end                             ,
       case
         when (
           mp_gas.meterpointtype = 'G' and (mp_gas.supplyenddate is null or mp_gas.supplyenddate >= current_date))
                 then 'Live'
         else 'Not Live' end                             ,
       rse.status                                        ,
       rsg.status                                        ,
       mp_elec.supplystartdate                           ,
       mp_elec.supplyenddate                             ,
       mp_gas.supplystartdate                            ,
       mp_gas.supplyenddate                              ,
       su.created_at                                     ,
       mp_elec.meter_point_id                            ,
       mp_elec.meterpointnumber                          ,
       mp_gas.meter_point_id                             ,
       mp_gas.meterpointnumber                           ,
       mt_elec.meter_id                                  ,
       mt_gas.meter_id                                   ,
       reg_elec.register_id                              ,
       reg_gas.register_id                               ,
       reg_elec.registers_eacaq                          ,
       reg_gas.registers_eacaq                           ,
       reg_elec.registers_registerreference              ,
       reg_gas.registers_registerreference               ,
       reg_elec.registers_tpr                            ,
       reg_gas.registers_tpr
