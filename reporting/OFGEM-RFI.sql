-- RFI report to produce the report and get no of customers for each region having atleast a elec and atleast a gas
-- To generate an RFI report follow the below steps,
--   1.Populate the ref_calculated_account_meterpoints with the sql in igloo-bible (igloo-Bible/bible-ensek-meterpoint-attributes-gas-elec.sql)
--   2.RFI_elec_gas report
--    2.1 Run the RFI sql to generate the data for the respective quarter(param -from 1st day after EOQ).
--    2.2 Run RFI-backing_data sql to generate backing data for the report.
--   3. RFI_dual report
--    3.1 Run the RFI_dual sql to generate the data for the respective quarter(param -from 1st day after EOQ).
--    3.2 Run the RFI_dual-backing_data sql to generate backing data for the report.

-- RFI --
select 'Igloo Energy'                                   as supplier_name,
       '2019-07-01'                                     as date,
       '1-Elec'                                         as tariff_uid,
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
  and ((rcam.supply_enddate_elec >= '2019-07-01'
	and rcam.supply_enddate_elec > rcam.supply_startdate_elec)
  or rcam.supply_enddate_elec is null)
  and rcam.usage_flag_elec = 'valid'

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
       '2019-07-01'                                        as date,
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
  and ((rcam.supply_enddate_gas >= '2019-07-01'
  and rcam.supply_enddate_gas > rcam.supply_startdate_gas)
  or rcam.supply_enddate_gas is null)
  and rcam.usage_flag_gas = 'valid'

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
		rthg.rate;

-- RFI-backing_data --
select 'Igloo Energy'                                   as supplier_name,
       '2019-07-01'                                        as date,
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
       rcam.account_id                                    as account_id,
--        count(distinct(rcam.account_id))                 as number_of_customer_accounts,
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
  and ((rcam.supply_enddate_elec >= '2019-07-01'
	and rcam.supply_enddate_elec > rcam.supply_startdate_elec)
  or rcam.supply_enddate_elec is null)
  and rcam.usage_flag_elec = 'valid'
-- and rcam.meterpoint_attribute_elec_gsp = '_A' and rate != 12.393;

-- group by case
--          when rcam.meterpoint_attribute_elec_gsp = '_A' then 'east_england'
--          when rcam.meterpoint_attribute_elec_gsp = '_B' then 'east_midlands'
--          when rcam.meterpoint_attribute_elec_gsp = '_C' then 'london'
--          when rcam.meterpoint_attribute_elec_gsp = '_D' then 'merseyside_and_north_wales'
--          when rcam.meterpoint_attribute_elec_gsp = '_E' then 'midlands'
--          when rcam.meterpoint_attribute_elec_gsp = '_F' then 'north_east'
--          when rcam.meterpoint_attribute_elec_gsp = '_G' then 'north_west'
--          when rcam.meterpoint_attribute_elec_gsp = '_H' then 'southern'
--          when rcam.meterpoint_attribute_elec_gsp = '_J' then 'south_east'
--          when rcam.meterpoint_attribute_elec_gsp = '_K' then 'south_wales'
--          when rcam.meterpoint_attribute_elec_gsp = '_L' then 'south_west'
--          when rcam.meterpoint_attribute_elec_gsp = '_M' then 'yorkshire'
--          when rcam.meterpoint_attribute_elec_gsp = '_N' then 'south_scotland'
--          when rcam.meterpoint_attribute_elec_gsp = '_P' then 'north_scotland'
--          else 'No Region Assigned'
--            end,
-- 				 rthe.rate

union
select 'Igloo Energy'                                   as supplier_name,
       '2019-07-01'                                        as date,
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
       rcam.account_id                                  as account_id,
--        count(distinct(rcam.account_id))                 as number_of_customer_accounts,
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
  and ((rcam.supply_enddate_gas >= '2019-07-01'
  and rcam.supply_enddate_gas > rcam.supply_startdate_gas)
  or rcam.supply_enddate_gas is null)
  and rcam.usage_flag_gas = 'valid'


-- RFI_dual
select 'Igloo Energy'                  as supplier_name,
       '2019-07-01'                       as date,
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
--          rcam.account_id
        count(distinct(rcam.account_id))                 as number_of_accounts
from ref_calculated_account_meterpoints rcam
 inner join ref_tariff_history_gas_ur rthg on rcam.account_id = rthg.account_id and rthg.end_date is null
where rcam.dual = 'dual_fuel'
  and ((rcam.supply_enddate_elec >= '2019-07-01'
	and rcam.supply_enddate_elec > rcam.supply_startdate_elec)
  or rcam.supply_enddate_elec is null)
  and rcam.usage_flag_elec = 'valid'
  and ((rcam.supply_enddate_gas >= '2019-07-01'
  and rcam.supply_enddate_gas > rcam.supply_startdate_gas)
  or rcam.supply_enddate_gas is null)
  and rcam.usage_flag_gas = 'valid'
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

-- RFI_dual-backing_data
select 'Igloo Energy'                  as supplier_name,
       '2019-07-01'                       as date,
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
         rcam.account_id
--         count(distinct(rcam.account_id))                 as number_of_accounts
from ref_calculated_account_meterpoints rcam
 inner join ref_tariff_history_gas_ur rthg on rcam.account_id = rthg.account_id and rthg.end_date is null
where rcam.dual = 'dual_fuel'
  and ((rcam.supply_enddate_elec >= '2019-07-01'
	and rcam.supply_enddate_elec > rcam.supply_startdate_elec)
  or rcam.supply_enddate_elec is null)
  and rcam.usage_flag_elec = 'valid'
  and ((rcam.supply_enddate_gas >= '2019-07-01'
  and rcam.supply_enddate_gas > rcam.supply_startdate_gas)
  or rcam.supply_enddate_gas is null)
  and rcam.usage_flag_gas = 'valid';