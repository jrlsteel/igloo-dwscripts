SELECT account_id,
       igloo_user_id,
       supply_postcode,
       account_status,
       meterpoint_type_elec,
       meterpoint_type_gas,
       dual,
       meterpoints_status_elec,
       meterpoints_status_gas,
       registration_status_elec,
       registration_status_gas,
       supply_startdate_elec,
       supply_enddate_elec,
       supply_startdate_gas,
       supply_enddate_gas,
       supply_contract_creation_date,
       meter_point_id_elec,
       meterpointnumber_elec,
       meter_point_id_gas,
       meterpointnumber_gas,
       meter_id_elec,
       meter_id_gas,
       register_id_elec,
       register_id_gas,
       register_elec_eacaq,
       register_gas_eacaq,
       register_elec_reference,
       register_gas_reference,
       register_elec_tpr,
       register_gas_tpr,
       meterpoint_attribute_elec_billing_status,
       meterpoint_attribute_elec_da,
       meterpoint_attribute_elec_dc,
       meterpoint_attribute_elec_distributor,
       meterpoint_attribute_elec_energisation_status,
       meterpoint_attribute_elec_et,
       meterpoint_attribute_elec_gain_supplier,
       meterpoint_attribute_elec_green_deal,
       meterpoint_attribute_elec_green_deal_active,
       meterpoint_attribute_elec_gsp,
       meterpoint_attribute_elec_is_cot,
       meterpoint_attribute_elec_is_prepay,
       meterpoint_attribute_elec_llf_indicator,
       meterpoint_attribute_elec_llf,
       meterpoint_attribute_elec_loss_objection,
       meterpoint_attribute_elec_loss_reg_trans_no,
       meterpoint_attribute_elec_measurement_class,
       meterpoint_attribute_elec_metering_type,
       meterpoint_attribute_elec_meter_make_model,
       meterpoint_attribute_elec_mop,
       meterpoint_attribute_elec_mtc,
       meterpoint_attribute_elec_mtc_related,
       meterpoint_attribute_elec_new_da,
       meterpoint_attribute_elec_new_dc,
       meterpoint_attribute_elec_new_mop,
       meterpoint_attribute_elec_no_of_digits,
       meterpoint_attribute_elec_objection_status,
       meterpoint_attribute_elec_old_da,
       meterpoint_attribute_elec_old_dc,
       meterpoint_attribute_elec_old_mop,
       meterpoint_attribute_elec_old_supplier,
       meterpoint_attribute_elec_old_supplier_da,
       meterpoint_attribute_elec_old_supplier_dc,
       meterpoint_attribute_elec_old_supplier_mop,
       meterpoint_attribute_elec_profile_class,
       meterpoint_attribute_elec_read_cycle,
       meterpoint_attribute_elec_reg_trans_no,
       meterpoint_attribute_elec_ssc,
       meterpoint_attribute_elec_supplier,
       meterpoint_attribute_elec_supply_status,
       meterpoint_attribute_elec_thresh_daily_cons,
       meterpoint_attribute_gas_client_unique_reference,
       meterpoint_attribute_gas_confirmation_reference,
       meterpoint_attribute_elec_dcgas_current_mam_abbrev_name,
       meterpoint_attribute_gas_et,
       meterpoint_attribute_gas_gain_supplier,
       meterpoint_attribute_gas_act_owner,
       meterpoint_attribute_gas_imperial_meter_indicator,
       meterpoint_attribute_gas_meter_location_code,
       meterpoint_attribute_gas_meter_manufacturer_year,
       meterpoint_attribute_gas_meter_manufacturer_code,
       meterpoint_attribute_gas_meter_mechanism,
       meterpoint_attribute_gas_meter_model,
       meterpoint_attribute_gas_meter_serial_number,
       meterpoint_attribute_gas_meter_status,
       meterpoint_attribute_gas_no_of_digits,
       meterpoint_attribute_gas_igt_indicator,
       meterpoint_attribute_gas_ldz,
       meterpoint_attribute_gas_large_site_indicator,
       meterpoint_attribute_gas_loss_objection,
       meterpoint_attribute_gas_new_mam,
       meterpoint_attribute_gas_nom_shipper_ref,
       meterpoint_attribute_gas_old_supplier,
       meterpoint_attribute_gas_old_supplier_mam,
       meterpoint_attribute_gas_objection_status,
       meterpoint_attribute_gas_supply_point_cat,
       meterpoint_attribute_gas_supply_status,
       meterpoint_attribute_gas_thresh_daily_consumption,
       meterpoint_attribute_gas_transporter,
       meterpoint_attribute_gas_isprepay,
       has_meterpoint_elec,
       has_meterpoint_gas,
       has_meter_elec,
       has_meter_gas,
       has_register_elec,
       has_register_gas,
       has_register_elec_eacaq,
       has_register_gas_eacaq,
       has_register_elec_registereference,
       has_register_gas_registerreference,
       has_register_elec_tpr,
       has_register_gas_tpr,
       has_meterpoint_attribute_elec_supply_status,
       has_meterpoint_attribute_elec_billing_status,
       has_meterpoint_attribute_elec_da,
       has_meterpoint_attribute_elec_dc,
       has_meterpoint_attribute_elec_distributor,
       has_meterpoint_attribute_elec_energisation_status,
       has_meterpoint_attribute_elec_et,
       has_meterpoint_attribute_elec_gain_supplier,
       has_meterpoint_attribute_elec_green_deal,
       has_meterpoint_attribute_elec_green_deal_active,
       has_meterpoint_attribute_elec_gsp,
       has_meterpoint_attribute_elec_is_cot,
       has_meterpoint_attribute_elec_is_prepay,
       has_meterpoint_attribute_elec_llf_indicator,
       has_meterpoint_attribute_elec_llf,
       has_meterpoint_attribute_elec_loss_objection,
       has_meterpoint_attribute_elec_loss_reg_trans_no,
       has_meterpoint_attribute_elec_measurement_class,
       has_meterpoint_attribute_elec_metering_type,
       has_meterpoint_attribute_elec_meter_make_model,
       has_meterpoint_attribute_elec_mop,
       has_meterpoint_attribute_elec_mtc,
       has_meterpoint_attribute_elec_mtc_related,
       has_meterpoint_attribute_elec_new_da,
       has_meterpoint_attribute_elec_new_dc,
       has_meterpoint_attribute_elec_new_mop,
       has_meterpoint_attribute_elec_no_of_digits,
       has_meterpoint_attribute_elec_objection_status,
       has_meterpoint_attribute_elec_old_da,
       has_meterpoint_attribute_elec_old_dc,
       has_meterpoint_attribute_elec_old_mop,
       has_meterpoint_attribute_elec_old_supplier,
       has_meterpoint_attribute_elec_old_supplier_da,
       has_meterpoint_attribute_elec_old_supplier_dc,
       has_meterpoint_attribute_elec_old_supplier_mop,
       has_meterpoint_attribute_elec_profile_class,
       has_meterpoint_attribute_elec_read_cycle,
       has_meterpoint_attribute_elec_reg_trans_no,
       has_meterpoint_attribute_elec_ssc,
       has_meterpoint_attribute_elec_supplier,
       has_meterpoint_attribute_elec_thresh_daily_cons,
       has_meterpoint_attribute_gas_client_unique_reference,
       has_meterpoint_attribute_gas_confirmation_reference,
       has_meterpoint_attribute_gas_current_mam_abbr_name,
       has_meterpoint_attribute_gas_et,
       has_meterpoint_attribute_gas_gain_supplier,
       has_meterpoint_attribute_gas_act_owner,
       has_meterpoint_attribute_gas_imperial_meter_indicator,
       has_meterpoint_attribute_gas_meter_location_code,
       has_meterpoint_attribute_gas_meter_manu_year,
       has_meterpoint_attribute_gas_meter_manu_code,
       has_meterpoint_attribute_gas_meter_mechanism,
       has_meterpoint_attribute_gas_meter_model,
       has_meterpoint_attribute_gas_meter_serial_number,
       has_meterpoint_attribute_gas_meter_status,
       has_meterpoint_attribute_gas_no_of_digits,
       has_meterpoint_attribute_gas_ldz,
       has_meterpoint_attribute_gas_large_site_indicator,
       has_meterpoint_attribute_gas_loss_objection,
       has_meterpoint_attribute_gas_new_mam,
       has_meterpoint_attribute_gas_nomination_shipper_ref,
       has_meterpoint_attribute_gas_old_supplier,
       has_meterpoint_attribute_gas_old_supplier_mam,
       has_meterpoint_attribute_objection_status,
       has_meterpoint_attribute_gas_supply_point_contact,
       has_meterpoint_attribute_gas_supply_status,
       has_meterpoint_attribute_gas_threshold_consumption,
       has_meterpoint_attribute_gas_transporter,
       has_meterpoint_attribute_gas_igt_indicator,
       has_meterpoint_attribute_gas_is_prepay,
       has_enddate_before_startdate_elec,
       has_enddate_before_startdate_gas
FROM ref_calculated_account_meterpoints

select 'Igloo Energy'                                   as supplier_name,
       getdate()                                        as date,
      '1-Elec'                                          as tariff_uid,
       'Igloo Pioneer'                                  as tariff_advertised_name,
       case
         when rcam.meterpoint_attribute_elec_gsp = '_A' then 'east_england'
         when rcam.meterpoint_attribute_elec_gsp = '_B' then 'east_midlands'
         when rcam.meterpoint_attribute_elec_gsp = '_C' then 'london'
         when rcam.meterpoint_attribute_elec_gsp = '_D' then 'merseyside_and_north_wales'
         when rcam.meterpoint_attribute_elec_gsp = '_E' then 'midlands'
         when rcam.meterpoint_attribute_elec_gsp = '_F' then 'north_east'
         when rcam.meterpoint_attribute_elec_gsp = '_G' then 'north_west'
         when rcam.meterpoint_attribute_elec_gsp = '_H' then 'southern'
         when rcam.meterpoint_attribute_elec_gsp = '_J' then 'south_east'
         when rcam.meterpoint_attribute_elec_gsp = '_K' then 'south_wales'
         when rcam.meterpoint_attribute_elec_gsp = '_L' then 'south_west'
         when rcam.meterpoint_attribute_elec_gsp = '_M' then 'yorkshire'
         when rcam.meterpoint_attribute_elec_gsp = '_N' then 'south_scotland'
         when rcam.meterpoint_attribute_elec_gsp = '_P' then 'north_scotland'
         else 'No Region Assigned'
           end                                          as region,
       'S'                                              as tariff_type,
       'E'                                              as tariff_fuel_type,
       'D'                                              as payment_method,
       'Y'                                              as online_account,
       'Y'                                              as paperless_billing,
       2.6                                              as renewable_percentage,
       'N '                                             as default_3_years,
       'N '                                             as default_customer_moved,
       count(distinct(rcam.account_id))                 as number_of_customer_accounts,
       'N'                                              as is_multi_reg_tariff,
       'N'                                              as is_multi_reg_tier_tariff,
       19.841                                           as standing_charge,
       rthe.rate                                        as single_rate_unit_rate,
       null                                             as multi_tier_volume_break_1,
       null                                             as multi_tier_volume_break_1_uom,
       null                                             as multi_tier_unit_rate_1,
       null                                             as multi_tier_unit_rate_2,
       null                                             as multi_tier_unit_rate_op,
       null                                             as assumed_consumption_split_1,
       null                                             as assumed_consumption_split_2,
       null                                             as multi_reg_period_1_unit_rate,
       null                                             as multi_reg_period_2_unit_rate,
       null                                             as dual_fuel_discount,
       null                                             as online_discount,
       0                                                as termination_fee,
       null                                             as fix_length,
       '2017-03-31'                                     as tariff_offer_date,
       null                                             as tariff_withdraw_date,
       null                                             as tariff_expiry_date,
       '2018-03-13'                                     as tariff_change_date
from ref_calculated_account_meterpoints rcam
     inner join ref_tariff_history_elec_ur rthe on rcam.account_id = rthe.account_id and rthe.end_date is null
where rcam.meterpoint_type_elec = 'E'
  and (rcam.supply_enddate_elec >= '2019-04-01' or rcam.supply_enddate_elec is null)
group by case
           when rcam.meterpoint_attribute_elec_gsp = '_A' then 'east_england'
           when rcam.meterpoint_attribute_elec_gsp = '_B' then 'east_midlands'
           when rcam.meterpoint_attribute_elec_gsp = '_C' then 'london'
           when rcam.meterpoint_attribute_elec_gsp = '_D' then 'merseyside_and_north_wales'
           when rcam.meterpoint_attribute_elec_gsp = '_E' then 'midlands'
           when rcam.meterpoint_attribute_elec_gsp = '_F' then 'north_east'
           when rcam.meterpoint_attribute_elec_gsp = '_G' then 'north_west'
           when rcam.meterpoint_attribute_elec_gsp = '_H' then 'southern'
           when rcam.meterpoint_attribute_elec_gsp = '_J' then 'south_east'
           when rcam.meterpoint_attribute_elec_gsp = '_K' then 'south_wales'
           when rcam.meterpoint_attribute_elec_gsp = '_L' then 'south_west'
           when rcam.meterpoint_attribute_elec_gsp = '_M' then 'yorkshire'
           when rcam.meterpoint_attribute_elec_gsp = '_N' then 'south_scotland'
           when rcam.meterpoint_attribute_elec_gsp = '_P' then 'north_scotland'
           else 'No Region Assigned'
    end,
  rthe.rate
union
select 'Igloo Energy'                                   as supplier_name,
       getdate()                                        as date,
       '1-GAS'                                          as tariff_uid,
       'Igloo Pioneer'                                  as tariff_advertised_name,
       case
         when rcam.meterpoint_attribute_elec_gsp = '_A' then 'east_england'
         when rcam.meterpoint_attribute_elec_gsp = '_B' then 'east_midlands'
         when rcam.meterpoint_attribute_elec_gsp = '_C' then 'london'
         when rcam.meterpoint_attribute_elec_gsp = '_D' then 'merseyside_and_north_wales'
         when rcam.meterpoint_attribute_elec_gsp = '_E' then 'midlands'
         when rcam.meterpoint_attribute_elec_gsp = '_F' then 'north_east'
         when rcam.meterpoint_attribute_elec_gsp = '_G' then 'north_west'
         when rcam.meterpoint_attribute_elec_gsp = '_H' then 'southern'
         when rcam.meterpoint_attribute_elec_gsp = '_J' then 'south_east'
         when rcam.meterpoint_attribute_elec_gsp = '_K' then 'south_wales'
         when rcam.meterpoint_attribute_elec_gsp = '_L' then 'south_west'
         when rcam.meterpoint_attribute_elec_gsp = '_M' then 'yorkshire'
         when rcam.meterpoint_attribute_elec_gsp = '_N' then 'south_scotland'
         when rcam.meterpoint_attribute_elec_gsp = '_P' then 'north_scotland'
         else 'No Region Assigned'
           end                                          as region,
       'S'                                              as tariff_type,
       'G'                                              as tariff_fuel_type,
       'D'                                              as payment_method,
       'Y'                                              as online_account,
       'Y'                                              as paperless_billing,
        0                                                 as renewable_percentage,
       'N '                                             as default_3_years,
       'N '                                             as default_customer_moved,
       count(distinct(rcam.account_id))                 as number_of_customer_accounts,
       'N'                                              as is_multi_reg_tariff,
       'N'                                              as is_multi_reg_tier_tariff,
       23.333                                           as standing_charge,
       rthg.rate                                        as single_rate_unit_rate,
       null                                             as multi_tier_volume_break_1,
       null                                             as multi_tier_volume_break_1_uom,
       null                                             as multi_tier_unit_rate_1,
       null                                             as multi_tier_unit_rate_2,
       null                                             as multi_tier_unit_rate_op,
       null                                             as assumed_consumption_split_1,
       null                                             as assumed_consumption_split_2,
       null                                             as multi_reg_period_1_unit_rate,
       null                                             as multi_reg_period_2_unit_rate,
       null                                             as dual_fuel_discount,
       null                                             as online_discount,
       0                                                as termination_fee,
       null                                             as fix_length,
       '2017-07-26'                                     as tariff_offer_date,
       null                                             as tariff_withdraw_date,
       null                                             as tariff_expiry_date,
       '2018-03-13'                                     as tariff_change_date
from ref_calculated_account_meterpoints rcam
     inner join ref_tariff_history_gas_ur rthg on rcam.account_id = rthg.account_id and rthg.end_date is null
where rcam.meterpoint_type_gas = 'G'
  and (rcam.supply_enddate_gas >= '2019-04-01' or rcam.supply_enddate_gas is null)
group by case
           when rcam.meterpoint_attribute_elec_gsp = '_A' then 'east_england'
           when rcam.meterpoint_attribute_elec_gsp = '_B' then 'east_midlands'
           when rcam.meterpoint_attribute_elec_gsp = '_C' then 'london'
           when rcam.meterpoint_attribute_elec_gsp = '_D' then 'merseyside_and_north_wales'
           when rcam.meterpoint_attribute_elec_gsp = '_E' then 'midlands'
           when rcam.meterpoint_attribute_elec_gsp = '_F' then 'north_east'
           when rcam.meterpoint_attribute_elec_gsp = '_G' then 'north_west'
           when rcam.meterpoint_attribute_elec_gsp = '_H' then 'southern'
           when rcam.meterpoint_attribute_elec_gsp = '_J' then 'south_east'
           when rcam.meterpoint_attribute_elec_gsp = '_K' then 'south_wales'
           when rcam.meterpoint_attribute_elec_gsp = '_L' then 'south_west'
           when rcam.meterpoint_attribute_elec_gsp = '_M' then 'yorkshire'
           when rcam.meterpoint_attribute_elec_gsp = '_N' then 'south_scotland'
           when rcam.meterpoint_attribute_elec_gsp = '_P' then 'north_scotland'
           else 'No Region Assigned'
    end,
  rthg.rate
;



select 'Igloo Energy'                  as supplier_name,
       getdate()                       as date,
       '1-Gas'                         as gas_tariff_uid_1,
       '1-Elec'                        as electricity_tariff_uid_1,
       null                             as electricity_tariff_uid_2,
       case
         when rcam.meterpoint_attribute_elec_gsp = '_A' then 'east_england'
         when rcam.meterpoint_attribute_elec_gsp = '_B' then 'east_midlands'
         when rcam.meterpoint_attribute_elec_gsp = '_C' then 'london'
         when rcam.meterpoint_attribute_elec_gsp = '_D' then 'merseyside_and_north_wales'
         when rcam.meterpoint_attribute_elec_gsp = '_E' then 'midlands'
         when rcam.meterpoint_attribute_elec_gsp = '_F' then 'north_east'
         when rcam.meterpoint_attribute_elec_gsp = '_G' then 'north_west'
         when rcam.meterpoint_attribute_elec_gsp = '_H' then 'southern'
         when rcam.meterpoint_attribute_elec_gsp = '_J' then 'south_east'
         when rcam.meterpoint_attribute_elec_gsp = '_K' then 'south_wales'
         when rcam.meterpoint_attribute_elec_gsp = '_L' then 'south_west'
         when rcam.meterpoint_attribute_elec_gsp = '_M' then 'yorkshire'
         when rcam.meterpoint_attribute_elec_gsp = '_N' then 'south_scotland'
         when rcam.meterpoint_attribute_elec_gsp = '_P' then 'north_scotland'
         else 'No Region Assigned'
           end                                          as region,
         'D' as payment_method_gas_1,
         'D' as payment_method_electricity_1,
         null as payment_method_electricity_2,
         'N' as default_3_years_gas_1,
         'N' as default_3_years_electricity_1,
         null as default_3_years_electricity_2,
        count(distinct(rcam.account_id))                 as number_of_accounts
from ref_calculated_account_meterpoints rcam
 inner join ref_tariff_history_gas_ur rthg on rcam.account_id = rthg.account_id and rthg.end_date is null
where rcam.dual = 'dual_fuel'
  and (rcam.supply_enddate_elec >= '2019-04-01' or rcam.supply_enddate_elec is null)
  and (rcam.supply_enddate_gas >= '2019-04-01' or rcam.supply_enddate_gas is null)
group by case
           when rcam.meterpoint_attribute_elec_gsp = '_A' then 'east_england'
           when rcam.meterpoint_attribute_elec_gsp = '_B' then 'east_midlands'
           when rcam.meterpoint_attribute_elec_gsp = '_C' then 'london'
           when rcam.meterpoint_attribute_elec_gsp = '_D' then 'merseyside_and_north_wales'
           when rcam.meterpoint_attribute_elec_gsp = '_E' then 'midlands'
           when rcam.meterpoint_attribute_elec_gsp = '_F' then 'north_east'
           when rcam.meterpoint_attribute_elec_gsp = '_G' then 'north_west'
           when rcam.meterpoint_attribute_elec_gsp = '_H' then 'southern'
           when rcam.meterpoint_attribute_elec_gsp = '_J' then 'south_east'
           when rcam.meterpoint_attribute_elec_gsp = '_K' then 'south_wales'
           when rcam.meterpoint_attribute_elec_gsp = '_L' then 'south_west'
           when rcam.meterpoint_attribute_elec_gsp = '_M' then 'yorkshire'
           when rcam.meterpoint_attribute_elec_gsp = '_N' then 'south_scotland'
           when rcam.meterpoint_attribute_elec_gsp = '_P' then 'north_scotland'
           else 'No Region Assigned'
    end
;


select 'Igloo Energy'                  as supplier_name,
       getdate()                       as date,
       '1-Gas'                         as gas_tariff_uid_1,
       '1-Elec'                        as electricity_tariff_uid_1,
       null                             as electricity_tariff_uid_2,
       case
         when mpa.attributes_attributevalue = '_A' then 'east_england'
         when mpa.attributes_attributevalue = '_B' then 'east_midlands'
         when mpa.attributes_attributevalue = '_C' then 'london'
         when mpa.attributes_attributevalue = '_D' then 'merseyside_and_north_wales'
         when mpa.attributes_attributevalue = '_E' then 'midlands'
         when mpa.attributes_attributevalue = '_F' then 'north_east'
         when mpa.attributes_attributevalue = '_G' then 'north_west'
         when mpa.attributes_attributevalue = '_H' then 'southern'
         when mpa.attributes_attributevalue = '_J' then 'south_east'
         when mpa.attributes_attributevalue = '_K' then 'south_wales'
         when mpa.attributes_attributevalue = '_L' then 'south_west'
         when mpa.attributes_attributevalue = '_M' then 'yorkshire'
         when mpa.attributes_attributevalue = '_N' then 'south_scotland'
         when mpa.attributes_attributevalue = '_P' then 'north_scotland'
           end                         as region,
       count(distinct(rth.account_id)) as number_customer_accounts
from ref_meterpoints mp
       inner join ref_meterpoints_attributes mpa on mp.account_id = mpa.account_id
       inner join ref_tariff_history rth on mpa.account_id = rth.account_id
       inner join ref_meterpoints mp1 on mp.account_id = mp1.account_id
       inner join vw_acl_reg_gaselec_happy vreh on mp.account_id = vreh.account_id
WHERE mp.meterpointtype = 'E'
  and (mp.supplyenddate is null or mp.supplyenddate >= '2019-04-01')
  and mp1.meterpointtype = 'G'
  and (mp1.supplyenddate is null or mp1.supplyenddate > '2019-04-01')
  and mpa.attributes_attributename = 'GSP'
group by mpa.attributes_attributevalue
