select su.external_id                                       as account_id,
       rcup.user_id                                         as igloo_user_id,
       rca.postcode                                         as supply_postcode,
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
       rse.status                                           as registration_status_elec,
       rsg.status                                           as registration_status_gas,
       mp_elec.supplystartdate                              as supply_startdate_elec,
       mp_elec.supplyenddate                                as supply_enddate_elec,
       mp_gas.supplystartdate                               as supply_startdate_gas,
       mp_gas.supplyenddate                                 as supply_enddate_gas,
       su.created_at                                        as supply_contract_creation_date,
       mp_elec.meter_point_id                               as meter_point_id_elec,
       mp_gas.meter_point_id                                as meter_point_id_gas,
       mt_elec.meter_id                                     as meter_id_elec,
       mt_gas.meter_id                                      as meter_id_gas,
       reg_elec.register_id                                 as register_id_elec,
       reg_gas.register_id                                  as register_id_gas,
       reg_elec.registers_eacaq                             as register_elec_eacaq,
       reg_gas.registers_eacaq                              as register_gas_eacaq,
       reg_elec.registers_registerreference                 as register_elec_reference,
       reg_gas.registers_registerreference                  as register_gas_reference,
       reg_elec.registers_tpr                               as register_elec_tpr,
       reg_gas.registers_tpr                                as register_gas_tpr,
    -- Meterpoint Attributes  Gas  Section
       max(case
             when mpa_gas.attributes_attributename = 'CLIENT_UNIQUE_REFERENCE' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_client_unique_reference,
       max(case
             when mpa_gas.attributes_attributename = 'Confirmation_Reference' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_confirmation_reference,
       max(case
             when mpa_gas.attributes_attributename = 'Current_Mam_Abbreviated_Name'
                     then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_dcgas_current_mam_abbrev_name,
       max(case
             when mpa_gas.attributes_attributename = 'ET' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_et,
       max(case
             when mpa_gas.attributes_attributename = 'GAIN_SUPPLIER' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_gain_supplier,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Act_Owner' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_act_owner,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Imperial_Meter_Indicator'
                     then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_imperial_meter_indicator,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Meter_Location_Code' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_meter_location_code,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Meter_Manufactured_Year'
                     then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_meter_manufacturer_year,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Meter_Manufacturer_Code'
                     then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_meter_manufacturer_code,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Meter_Mechanism' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_meter_mechanism,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Meter_Model' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_meter_model,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Meter_Serial_Number' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_meter_serial_number,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Meter_Status' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_meter_status,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_No_Of_Digits' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_no_of_digits,
       max(case
             when mpa_gas.attributes_attributename = 'IGT Indicator' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_igt_indicator,
       max(case
             when mpa_gas.attributes_attributename = 'LDZ'
                     then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_ldz,
       max(case
             when mpa_gas.attributes_attributename = 'Large Site Indicator' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_large_site_indicator,
       max(case
             when mpa_gas.attributes_attributename = 'Loss Objection' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_loss_objection,
       max(case
             when mpa_gas.attributes_attributename = 'NEW_MAM' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_new_mam,
       max(case
             when mpa_gas.attributes_attributename = 'NOMINATION_SHIPPER_REF' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_nom_shipper_ref,
       max(case
             when mpa_gas.attributes_attributename = 'OLD_SUPPLIER' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_old_supplier,
       max(case
             when mpa_gas.attributes_attributename = 'OLD_SUPPLIER_MAM' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_old_supplier_mam,
       max(case
             when mpa_gas.attributes_attributename = 'Objection Status' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_objection_status,
       max(case
             when mpa_gas.attributes_attributename = 'SUPPLY_POINT_CATEGORY' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_supply_point_cat,
       max(case
             when mpa_gas.attributes_attributename = 'Supply_Status' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_supply_status,
       max(case
             when mpa_gas.attributes_attributename = 'Threshold.DailyConsumption' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_thresh_daily_consumption,
       max(case
             when mpa_gas.attributes_attributename = 'Transporter' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_transporter,
       max(case
             when mpa_gas.attributes_attributename = 'igtIndicator' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_igt_indicator,
       max(case
             when mpa_gas.attributes_attributename = 'isPrepay' then mpa_gas.attributes_attributevalue
               end)                                         as meterpoint_attribute_gas_isPrePay,
    --Start of indciators
       max(case
             when (mt_elec.meter_id is not null) then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_elec,
       max(case
             when (mp_gas.meter_point_id is not null) then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_gas,
       max(case
             when (mt_elec.meter_id is not null) then 1
             else 0 end) over (partition by su.external_id) as has_meter_elec,
       max(case
             when (mt_gas.meter_id is not null) then 1
             else 0 end) over (partition by su.external_id) as has_meter_gas,
       max(case
             when (reg_elec.register_id is not null) then 1
             else 0 end) over (partition by su.external_id) as has_register_elec,
       max(case
             when (reg_gas.register_id is not null) then 1
             else 0 end) over (partition by su.external_id) as has_register_gas,
       max(case
             when (reg_elec.registers_eacaq is not null) then 1
             else 0 end) over (partition by su.external_id) as has_register_elec_eacaq,
       max(case
             when (reg_gas.registers_eacaq is not null) then 1
             else 0 end) over (partition by su.external_id) as has_register_gas_eacaq,
       max(case
             when (reg_elec.registers_registerreference is not null) then 1
             else 0 end) over (partition by su.external_id) as has_register_elec_registereference,
       max(case
             when (reg_gas.registers_registerreference is not null) then 1
             else 0 end) over (partition by su.external_id) as has_register_gas_registerreference,
       max(case
             when (reg_elec.registers_tpr is not null) then 1
             else 0 end) over (partition by su.external_id) as has_register_elec_tpr,
       max(case
             when (reg_gas.registers_tpr is not null) then 1
             else 0 end) over (partition by su.external_id) as has_register_gas_tpr,
-- Meterpoint Attributes Elec Indicators
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'CLIENT_UNIQUE_REFERENCE' and
                              mpa_gas.attributes_attributevalue is not null) then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_client_unique_reference,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Confirmation_Reference' and
                              mpa_gas.attributes_attributevalue is not null) then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_confirmation_reference,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Current_Mam_Abbreviated_Name' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_current_mam_abbr_name,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'ET' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_et,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'GAIN_SUPPLIER' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_gain_supplier,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Act_Owner' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_act_owner,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Imperial_Meter_Indicator' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_imperial_meter_indicator,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Meter_Location_Code' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_meter_location_code,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Meter_Manufactured_Year' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_meter_manu_year,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Meter_Manufacturer_Code' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_meter_manu_code,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Meter_Mechanism' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_meter_mechanism,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Meter_Model' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_meter_model,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Meter_Serial_Number' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_meter_serial_number,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_Meter_Status' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_meter_status,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Gas_No_Of_Digits' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_no_of_digits,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'IGT Indicator' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_igt_indicator,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'LDZ' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_ldz,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Large Site Indicator'
                                and mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_large_site_indicator,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Loss Objection' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_loss_objection,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'NEW_MAM' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_new_mam,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'NOMINATION_SHIPPER_REF' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_nomination_shipper_ref,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'OLD_SUPPLIER' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_old_supplier,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'OLD_SUPPLIER_MAM' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_old_supplier_mam,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Objection Status' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_objection_status,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'SUPPLY_POINT_CATEGORY' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_supply_point_contact,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Supply_Status' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_supply_status,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Threshold.DailyConsumption' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_threshold_consumption,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'Transporter' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_transporter,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'igtIndicator' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_igt_indicator,
       max(case
             when max(case
                        when (mpa_gas.attributes_attributename = 'isPrepay' and
                              mpa_gas.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_gas_is_prepay,
       max(case
             when (mp_elec.supplystartdate > mp_elec.supplyenddate)
                     then 1
             else 0 end)
           over (partition by su.external_id)               as has_enddate_before_startdate_elec,
       max(case
             when (mp_gas.supplystartdate > mp_gas.supplyenddate) then 1
             else 0 end) over (partition by su.external_id) as has_enddate_before_startdate_gas
from   ref_cdb_supply_contracts su --inner join temp_cab_dates_quarter4_2018 we on we.external_id = su.external_id
       inner join ref_cdb_user_permissions rcup on su.id = rcup.permissionable_id and permission_level = 0 and  permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_users rcu on rcup.user_id = rcu.id
       inner join ref_cdb_addresses rca on su.supply_address_id = rca.id

       --Ensek Meterpoint Elec
       left outer join ref_meterpoints mp_elec on mp_elec.account_id = su.external_id and mp_elec.meterpointtype = 'E'
       left outer join ref_meterpoints_attributes mpa_elec on mp_elec.account_id = mpa_elec.account_id and mp_elec.meter_point_id = mpa_elec.meter_point_id and attributes_effectivetodate is null
       left outer join ref_meters mt_elec on mp_elec.account_id = mt_elec.account_id and mp_elec.meter_point_id = mt_elec.meter_point_id and mt_elec.removeddate is null
       --left outer join ref_meters_attributes mta_elec on mt_elec.meter_id = mta_elec.meter_id
       left outer join ref_registers reg_elec on mt_elec.account_id = reg_elec.account_id and mt_elec.meter_id = reg_elec.meter_id

       --Ensek Meterpoint Gas
       left outer join ref_meterpoints mp_gas on mp_gas.account_id = su.external_id and mp_gas.meterpointtype = 'G'
       left outer join ref_meterpoints_attributes mpa_gas on mp_gas.account_id = mpa_gas.account_id and mp_gas.meter_point_id = mpa_gas.meter_point_id and mpa_gas.attributes_effectivetodate is null
       left outer join ref_meters mt_gas on mp_gas.account_id = mt_gas.account_id and mp_gas.meter_point_id = mt_gas.meter_point_id and mt_gas.removeddate is null
       left outer join ref_registers reg_gas on mt_gas.account_id = reg_gas.account_id and mt_gas.meter_id = reg_gas.meter_id
       --left outer join ref_registers_attributes rga_elec on reg_elec.register_id = rga_elec.register_id

       --Status
       left outer join ref_account_status ac on ac.account_id = su.external_id
       left outer join ref_registrations_status_gas rsg on mp_elec.account_id = rsg.account_id
       left outer join ref_registrations_status_elec rse on mp_elec.account_id = rse.account_id
 --     where su.external_id = 11993
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




-- select distinct(attributes_attributename)
-- from ref_meterpoints_attributes rma
--      inner join ref_meterpoints rm on rm.account_id = rma.account_id and rm.meter_point_id=rma.meter_point_id
-- where rm.meterpointtype ='E'
-- order by rma.attributes_attributename

