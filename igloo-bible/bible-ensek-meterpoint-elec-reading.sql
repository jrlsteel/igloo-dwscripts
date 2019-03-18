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
    -- Meterpoint Attributes  Electricity Section
       max(case
             when mpa_elec.attributes_attributename = 'BILLING STATUS' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_billing_status,
       max(case
             when mpa_elec.attributes_attributename = 'DA' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_da,
       max(case
             when mpa_elec.attributes_attributename = 'DC' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_dc,
       max(case
             when mpa_elec.attributes_attributename = 'Distributor' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_distributor,
       max(case
             when mpa_elec.attributes_attributename = 'EnergisationStatus' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_energisation_status,
       max(case
             when mpa_elec.attributes_attributename = 'ET' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_et,
       max(case
             when mpa_elec.attributes_attributename = 'GAIN_SUPPLIER' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_gain_supplier,
       max(case
             when mpa_elec.attributes_attributename = 'Green Deal' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_green_deal,
       max(case
             when mpa_elec.attributes_attributename = 'greenDealActive' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_green_deal_active,
       max(case
             when mpa_elec.attributes_attributename = 'GSP' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_gsp,
       max(case
             when mpa_elec.attributes_attributename = 'isCOT' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_is_cot,
       max(case
             when mpa_elec.attributes_attributename = 'IsPrepay' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_is_prepay,
       max(case
             when mpa_elec.attributes_attributename = 'isPrepay' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_is_prepay,
       max(case
             when mpa_elec.attributes_attributename = 'LLF Indicator' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_llf_indicator,
       max(case
             when mpa_elec.attributes_attributename = 'LLFC' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_llf,
       max(case
             when mpa_elec.attributes_attributename = 'Loss Objection' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_loss_objection,
       max(case
             when mpa_elec.attributes_attributename = 'LOSS_REGISTRATION_TRANSACTION_NUMBER'
                     then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_loss_reg_trans_no,
       max(case
             when mpa_elec.attributes_attributename = 'Measurement Class' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_measurement_class,
       max(case
             when mpa_elec.attributes_attributename = 'Metering Type' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_metering_type,
       max(case
             when mpa_elec.attributes_attributename = 'MeterMakeAndModel' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_meter_make_model,
       max(case
             when mpa_elec.attributes_attributename = 'MOP' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_mop,
       max(case
             when mpa_elec.attributes_attributename = 'MTC' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_mtc,
       max(case
             when mpa_elec.attributes_attributename = 'MTC Related' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_mtc_related,
       max(case
             when mpa_elec.attributes_attributename = 'NEW_DA' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_new_da,
       max(case
             when mpa_elec.attributes_attributename = 'NEW_DC' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_new_dc,
       max(case
             when mpa_elec.attributes_attributename = 'NEW_MOP' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_new_mop,
       max(case
             when mpa_elec.attributes_attributename = 'No_Of_Digits' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_no_of_digits,
       max(case
             when mpa_elec.attributes_attributename = 'Objection Status' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_objection_status,
       max(case
             when mpa_elec.attributes_attributename = 'OLD_DA' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_old_da,
       max(case
             when mpa_elec.attributes_attributename = 'OLD_DC' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_old_dc,
       max(case
             when mpa_elec.attributes_attributename = 'OLD_MOP' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_old_mop,
       max(case
             when mpa_elec.attributes_attributename = 'OLD_SUPPLIER' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_old_supplier,
       max(case
             when mpa_elec.attributes_attributename = 'OLD_SUPPLIER_DA' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_old_supplier_da,
       max(case
             when mpa_elec.attributes_attributename = 'OLD_SUPPLIER_DC' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_old_supplier_dc,
       max(case
             when mpa_elec.attributes_attributename = 'OLD_SUPPLIER_MOP' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_old_supplier_mop,
       max(case
             when mpa_elec.attributes_attributename = 'Profile Class' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_profile_class,
       max(case
             when mpa_elec.attributes_attributename = 'ReadCycle' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_read_cycle,
       max(case
             when mpa_elec.attributes_attributename = 'Registration_Transaction_Number'
                     then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_reg_trans_no,
       max(case
             when mpa_elec.attributes_attributename = 'SSC' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_ssc,
       max(case
             when mpa_elec.attributes_attributename = 'SUPPLIER' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_supplier,
       max(case
             when mpa_elec.attributes_attributename = 'Supply_Status' then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_supply_status,
       max(case
             when mpa_elec.attributes_attributename = 'Threshold.DailyConsumption'
                     then mpa_elec.attributes_attributevalue
               end)                                         as meterpoint_attribute_elec_thresh_daily_cons,
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
                        when (mpa_elec.attributes_attributename = 'Supply_Status' and
                              mpa_elec.attributes_attributevalue is not null) then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_supply_status,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'BILLING STATUS' and
                              mpa_elec.attributes_attributevalue is not null) then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_billing_status,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'DA' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_da,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'DC' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_dc,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Distributor' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_distributor,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'EnergisationStatus' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_energisation_status,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'ET' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_et,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'GAIN_SUPPLIER' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_gain_supplier,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Green Deal' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_green_deal,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'greenDealActive' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_green_deal_active,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'GSP' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_gsp,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'isCOT' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_is_cot,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'IsPrepay' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_is_prepay,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'isPrepay' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_is_prepay,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'LLF Indicator' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_llf_indicator,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'LLFC' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_llf,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Loss Objection' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_loss_objection,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'LOSS_REGISTRATION_TRANSACTION_NUMBER'
                                and mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_loss_reg_trans_no,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Measurement Class' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_measurement_class,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Metering Type' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_metering_type,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'MeterMakeAndModel' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_meter_make_model,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'MOP' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_mop,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'MTC' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_mtc,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'MTC Related' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_mtc_related,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'NEW_DA' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_new_da,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'NEW_DC' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_new_dc,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'NEW_MOP' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_new_mop,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'No_Of_Digits' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_no_of_digits,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Objection Status' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_objection_status,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'OLD_DA' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_old_da,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'OLD_DC' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_old_dc,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'OLD_MOP' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_old_mop,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'OLD_SUPPLIER' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_old_supplier,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'OLD_SUPPLIER_DA' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_old_supplier_da,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'OLD_SUPPLIER_DC' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_old_supplier_dc,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'OLD_SUPPLIER_MOP' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_old_supplier_mop,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Profile Class' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_profile_class,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'ReadCycle' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_read_cycle,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Registration_Transaction_Number'
                                and mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_reg_trans_no,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'SSC' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_ssc,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'SUPPLIER' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_supplier,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Supply_Status' and
                              mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_supply_status,
       max(case
             when max(case
                        when (mpa_elec.attributes_attributename = 'Threshold.DailyConsumption'
                                and mpa_elec.attributes_attributevalue is not null)
                                then 1
                        else 0
                          end) > 0 then 1
             else 0 end) over (partition by su.external_id) as has_meterpoint_attribute_elec_thresh_daily_cons,
       max(case
             when (mp_elec.supplystartdate > mp_elec.supplyenddate)
                     then 1
             else 0 end)
           over (partition by su.external_id)               as has_enddate_before_startdate_elec,
       max(case
             when (mp_gas.supplystartdate > mp_gas.supplyenddate) then 1
             else 0 end) over (partition by su.external_id) as has_enddate_before_startdate_gas,
           vidhcr.read_min_created_date_elec,
         vidhcr.read_max_created_date_elec,
         vidhcr.read_min_readings_elec,
         vidhcr.read_max_readings_elec,
         vidhcr.read_months_diff_elec,
         vidhcr.read_days_diff_elec,
         vidhcr.read_consumption_elec,
         vidhcr.industry_eac,
         vidhcr.tpr,
         vidhcr.d18_st_date,
         vidhcr.ppc_sum,
         vidhcr.igloo_eac,
         vidhcr.igloo_edc,
         vidhcr.igloo_edhc_1,
         vidhcr.igloo_edhc_2,
         vidhcr.igloo_edhc_3,
         vidhcr.igloo_edhc_4,
         vidhcr.igloo_edhc_5,
         vidhcr.igloo_edhc_6,
         vidhcr.igloo_edhc_7,
         vidhcr.igloo_edhc_8,
         vidhcr.igloo_edhc_9,
         vidhcr.igloo_edhc_10,
         vidhcr.igloo_edhc_11,
         vidhcr.igloo_edhc_12,
         vidhcr.igloo_edhc_13,
         vidhcr.igloo_edhc_14,
         vidhcr.igloo_edhc_15,
         vidhcr.igloo_edhc_16,
         vidhcr.igloo_edhc_17,
         vidhcr.igloo_edhc_18,
         vidhcr.igloo_edhc_19,
         vidhcr.igloo_edhc_20,
         vidhcr.igloo_edhc_21,
         vidhcr.igloo_edhc_22,
         vidhcr.igloo_edhc_23,
         vidhcr.igloo_edhc_24,
         vidhcr.igloo_edhc_25,
         vidhcr.igloo_edhc_26,
         vidhcr.igloo_edhc_27,
        vidhcr.igloo_edhc_28,
         vidhcr.igloo_edhc_29,
         vidhcr.igloo_edhc_30,
         vidhcr.igloo_edhc_31,
         vidhcr.igloo_edhc_32,
         vidhcr.igloo_edhc_33,
         vidhcr.igloo_edhc_34,
         vidhcr.igloo_edhc_35,
         vidhcr.igloo_edhc_36,
         vidhcr.igloo_edhc_37,
         vidhcr.igloo_edhc_38,
         vidhcr.igloo_edhc_39,
         vidhcr.igloo_edhc_40,
         vidhcr.igloo_edhc_41,
         vidhcr.igloo_edhc_42,
         vidhcr.igloo_edhc_43,
         vidhcr.igloo_edhc_44,
         vidhcr.igloo_edhc_45,
         vidhcr.igloo_edhc_46,
         vidhcr.igloo_edhc_47,
         vidhcr.igloo_edhc_48
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
       left outer join ref_registers reg_elec on mt_elec.meter_id = reg_elec.meter_id
       left outer join ref_meterpoints mp_gas on mp_gas.account_id = su.external_id and mp_gas.meterpointtype = 'G'
       left outer join ref_meters mt_gas on mp_gas.meter_point_id = mt_gas.meter_point_id and mt_gas.removeddate is null
       left outer join ref_registers reg_gas on mt_gas.meter_id = reg_gas.meter_id
       left outer join ref_registrations_status_gas rsg on mp_gas.account_id = rsg.account_id
       left outer join ref_registrations_status_elec rse on mp_elec.account_id = rse.account_id
       left outer join  temp_igloo_daily_and_hourly_calculated_reads vidhcr on reg_elec.register_id = vidhcr.register_id
--where su.external_id = 23983
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
         reg_gas.registers_tpr,
         vidhcr.read_min_created_date_elec,
         vidhcr.read_max_created_date_elec,
         vidhcr.read_min_readings_elec,
         vidhcr.read_max_readings_elec,
         vidhcr.read_months_diff_elec,
         vidhcr.read_days_diff_elec,
         vidhcr.read_consumption_elec,
         vidhcr.industry_eac,
         vidhcr.tpr,
         vidhcr.d18_st_date,
         vidhcr.ppc_sum,
         vidhcr.igloo_eac,
         vidhcr.igloo_edc,
         vidhcr.igloo_edhc_1,
         vidhcr.igloo_edhc_2,
         vidhcr.igloo_edhc_3,
         vidhcr.igloo_edhc_4,
         vidhcr.igloo_edhc_5,
         vidhcr.igloo_edhc_6,
         vidhcr.igloo_edhc_7,
         vidhcr.igloo_edhc_8,
         vidhcr.igloo_edhc_9,
         vidhcr.igloo_edhc_10,
         vidhcr.igloo_edhc_11,
         vidhcr.igloo_edhc_12,
         vidhcr.igloo_edhc_13,
         vidhcr.igloo_edhc_14,
         vidhcr.igloo_edhc_15,
         vidhcr.igloo_edhc_16,
         vidhcr.igloo_edhc_17,
         vidhcr.igloo_edhc_18,
         vidhcr.igloo_edhc_19,
         vidhcr.igloo_edhc_20,
         vidhcr.igloo_edhc_21,
         vidhcr.igloo_edhc_22,
         vidhcr.igloo_edhc_23,
         vidhcr.igloo_edhc_24,
         vidhcr.igloo_edhc_25,
         vidhcr.igloo_edhc_26,
         vidhcr.igloo_edhc_27,
        vidhcr.igloo_edhc_28,
         vidhcr.igloo_edhc_29,
         vidhcr.igloo_edhc_30,
         vidhcr.igloo_edhc_31,
         vidhcr.igloo_edhc_32,
         vidhcr.igloo_edhc_33,
         vidhcr.igloo_edhc_34,
         vidhcr.igloo_edhc_35,
         vidhcr.igloo_edhc_36,
         vidhcr.igloo_edhc_37,
         vidhcr.igloo_edhc_38,
         vidhcr.igloo_edhc_39,
         vidhcr.igloo_edhc_40,
         vidhcr.igloo_edhc_41,
         vidhcr.igloo_edhc_42,
         vidhcr.igloo_edhc_43,
         vidhcr.igloo_edhc_44,
         vidhcr.igloo_edhc_45,
         vidhcr.igloo_edhc_46,
         vidhcr.igloo_edhc_47,
         vidhcr.igloo_edhc_48
    --MeterPoints Attributes Elec
order by su.external_id





-- select distinct(attributes_attributename)
-- from ref_meterpoints_attributes rma
--      inner join ref_meterpoints rm on rm.account_id = rma.account_id and rm.meter_point_id=rma.meter_point_id
-- where rm.meterpointtype ='E'
-- order by rma.attributes_attributename


SELECT account_id,
       elec_gsp,
       elec_ssc,
       meterpoint_id,
       supplyend_date,
       meter_removed_date,
       register_id,
       no_of_digits,
       read_min_created_date_elec,
       read_max_created_date_elec,
       read_min_readings_elec,
       read_max_readings_elec,
       read_months_diff_elec,
       read_days_diff_elec,
       read_consumption_elec,
       industry_eac,
       tpr,
       d18_st_date,
       ppc_sum,
       igloo_eac,
       igloo_edc,
       igloo_edhc_1,
       igloo_edhc_2,
       igloo_edhc_3,
       igloo_edhc_4,
       igloo_edhc_5,
       igloo_edhc_6,
       igloo_edhc_7,
       igloo_edhc_8,
       igloo_edhc_9,
       igloo_edhc_10,
       igloo_edhc_11,
       igloo_edhc_12,
       igloo_edhc_13,
       igloo_edhc_14,
       igloo_edhc_15,
       igloo_edhc_16,
       igloo_edhc_17,
       igloo_edhc_18,
       igloo_edhc_19,
       igloo_edhc_20,
       igloo_edhc_21,
       igloo_edhc_22,
       igloo_edhc_23,
       igloo_edhc_24,
       igloo_edhc_25,
       igloo_edhc_26,
       igloo_edhc_27,
       igloo_edhc_28,
       igloo_edhc_29,
       igloo_edhc_30,
       igloo_edhc_31,
       igloo_edhc_32,
       igloo_edhc_33,
       igloo_edhc_34,
       igloo_edhc_35,
       igloo_edhc_36,
       igloo_edhc_37,
       igloo_edhc_38,
       igloo_edhc_39,
       igloo_edhc_40,
       igloo_edhc_41,
       igloo_edhc_42,
       igloo_edhc_43,
       igloo_edhc_44,
       igloo_edhc_45,
       igloo_edhc_46,
       igloo_edhc_47,
       igloo_edhc_48,
       etlchange
FROM vw_igloo_daily_and_hourly_calculated_reads
