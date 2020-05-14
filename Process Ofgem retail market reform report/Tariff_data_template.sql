select distinct
     supplier_name  ,
     date  ,
     tariff_uid  ,
     tariff_advertised_name  ,
     region  ,
     meter_type  ,
     tariff_type  ,
     tariff_fuel_type  ,
     payment_method  ,
     online_account  ,
     paperless_billing  ,
     renewable_percentage  ,
     default_3_years  ,
     number_of_customer_accounts  ,
     is_multi_reg_tariff  ,
     is_multi_tier_tariff  ,
     standing_charge  ,
     single_rate_unit_rate  ,
     multi_tier_volume_break_1  ,
     multi_tier_volume_break_2  ,
     multi_tier_volume_break_3  ,
     multi_tier_volume_break_4  ,
     multi_tier_volume_break_1_uom  ,
     multi_tier_volume_break_2_uom  ,
     multi_tier_volume_break_3_uom  ,
     multi_tier_volume_break_4_uom  ,
     multi_tier_unit_rate_1  ,
     multi_tier_unit_rate_2  ,
     multi_tier_unit_rate_3  ,
     multi_tier_unit_rate_4  ,
     multi_tier_unit_rate_5  ,
     multi_tier_unit_rate_op  ,
     multi_tier_unit_rate_op_2  ,
     multi_tier_unit_rate_op_3  ,
     assumed_consumption_split_1  ,
     assumed_consumption_split_2  ,
     assumed_consumption_split_3  ,
     assumed_consumption_split_4  ,
     assumed_consumption_split_5  ,
     multi_reg_period_1_unit_rate  ,
     multi_reg_period_2_unit_rate  ,
     multi_reg_period_3_unit_rate  ,
     multi_reg_period_4_unit_rate  ,
     multi_reg_period_5_unit_rate  ,
     dual_fuel_discount  ,
     online_discount  ,
     termination_fee  ,
     fix_length  ,
     tariff_offer_date  ,
     tariff_withdraw_date  ,
     tariff_expiry_date  ,
     tariff_change_date

from (select 'Igloo Energy'                   as supplier_name,
             getdate()                        as date,
             '1-Elec'                         as tariff_uid,
             rth.tariff_name                     tariff_advertised_name,
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
                 end                          as region,
             case
               when mpa2.attributes_attributevalue = 1 then 'U'
               else  'O'
              end                             as meter_type,
             'S'                              as tariff_type,
             'E'                              as tariff_fuel_type,
             'D'                              as payment_method,
             'Y'                              as online_account,
             'Y'                              as paperless_billing,
             2.6                              as renewable_percentage,
             'N'                         as default_3_years, -- needs looking at
             count(distinct(rthe.account_id)) as number_of_customer_accounts,
             'N'                         as is_multi_reg_tariff,
             'N'                         as is_multi_tier_tariff,
             sthe.rate                        as standing_charge,
             rthe.rate                        as single_rate_unit_rate,
              null as   multi_tier_volume_break_1  ,
              null as   multi_tier_volume_break_2  ,
              null as   multi_tier_volume_break_3  ,
              null as   multi_tier_volume_break_4  ,
              null as   multi_tier_volume_break_1_uom  ,
              null as   multi_tier_volume_break_2_uom  ,
              null as   multi_tier_volume_break_3_uom  ,
              null as   multi_tier_volume_break_4_uom  ,
              null as   multi_tier_unit_rate_1  ,
              null as   multi_tier_unit_rate_2  ,
              null as   multi_tier_unit_rate_3  ,
              null as   multi_tier_unit_rate_4  ,
              null as   multi_tier_unit_rate_5  ,
              null as   multi_tier_unit_rate_op  ,
              null as   multi_tier_unit_rate_op_2  ,
              null as   multi_tier_unit_rate_op_3  ,
              null as   assumed_consumption_split_1  ,
              null as   assumed_consumption_split_2  ,
              null as   assumed_consumption_split_3  ,
              null as   assumed_consumption_split_4  ,
              null as   assumed_consumption_split_5  ,
              null as   multi_reg_period_1_unit_rate  ,
              null as   multi_reg_period_2_unit_rate  ,
              null as   multi_reg_period_3_unit_rate  ,
              null as   multi_reg_period_4_unit_rate  ,
              null as   multi_reg_period_5_unit_rate  ,
              null as   dual_fuel_discount  ,
              null as   online_discount,
              0    as   termination_fee,
             null  as	fix_length,
            '31/03/2017' as	tariff_offer_date,
            null as	tariff_withdraw_date,
            null as	tariff_expiry_date,
            '01/06/2019' as 	tariff_change_date

      FROM ref_meterpoints mp
             inner join ref_meterpoints_attributes mpa on mp.account_id = mpa.account_id
             inner join ref_meterpoints_attributes mpa2 on mp.account_id = mpa2.account_id
             inner join ref_tariff_history rth on mpa.account_id = rth.account_id
             inner join ref_tariff_history_elec_ur rthe on rth.account_id = rthe.account_id
             inner join ref_tariff_history_elec_sc sthe on rth.account_id = sthe.account_id
             inner join vw_acl_reg_elec_happy vreh on mp.account_id = vreh.account_id
      where mpa.attributes_attributename = 'GSP'
        and mpa2.attributes_attributename = 'Profile Class'
        and (mp.supplyenddate is null or mp.supplyenddate > '2020-03-31')
        and rth.end_date is null
        and rthe.end_date is null
        and sthe.end_date is null
        and mp.meterpointtype = 'E'
      group by rth.tariff_name,
               rth.tariff_type,
               mpa.attributes_attributevalue,
               case
               when mpa2.attributes_attributevalue = 1 then 'U'
               else  'O'
               end ,
               sthe.rate,
               rthe.rate

      union

      select 'Igloo Energy'                   as supplier_name,
             getdate()                        as date,
             '1-Gas'                          as tariff_uid,
             rth.tariff_name                     tariff_advertised_name,
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
                 end                          as region,
             'U'               as meter_type,
             'S'                        tariff_type,
             'G'                        as tariff_fuel_type,
             'D'               as payment_method,
             'Y'                        as online_account,
             'Y'                        as paperless_billing,
             0                                as renewable_percentage,
             'N'                         as default_3_years, -- needs looking at
             count(distinct(rthe.account_id)) as number_of_customer_accounts,
             'N'                         as is_multi_reg_tariff,
             'N'                         as is_multi_tier_tariff,
             sthe.rate                        as standing_charge,
             rthe.rate                        as single_rate_unit_rate,
              null as   multi_tier_volume_break_1  ,
              null as   multi_tier_volume_break_2  ,
              null as   multi_tier_volume_break_3  ,
              null as   multi_tier_volume_break_4  ,
              null as   multi_tier_volume_break_1_uom  ,
              null as   multi_tier_volume_break_2_uom  ,
              null as   multi_tier_volume_break_3_uom  ,
              null as   multi_tier_volume_break_4_uom  ,
              null as   multi_tier_unit_rate_1  ,
              null as   multi_tier_unit_rate_2  ,
              null as   multi_tier_unit_rate_3  ,
              null as   multi_tier_unit_rate_4  ,
              null as   multi_tier_unit_rate_5  ,
              null as   multi_tier_unit_rate_op  ,
              null as   multi_tier_unit_rate_op_2  ,
              null as   multi_tier_unit_rate_op_3  ,
              null as   assumed_consumption_split_1  ,
              null as   assumed_consumption_split_2  ,
              null as   assumed_consumption_split_3  ,
              null as   assumed_consumption_split_4  ,
              null as   assumed_consumption_split_5  ,
              null as   multi_reg_period_1_unit_rate  ,
              null as   multi_reg_period_2_unit_rate  ,
              null as   multi_reg_period_3_unit_rate  ,
              null as   multi_reg_period_4_unit_rate  ,
              null as   multi_reg_period_5_unit_rate  ,
              null as   dual_fuel_discount  ,
              null as   online_discount ,
              0    as   termination_fee,
             null  as	fix_length,
            '31/03/2017' as	tariff_offer_date,
            null as	tariff_withdraw_date,
            null as	tariff_expiry_date,
            '01/06/2019' as 	tariff_change_date

      FROM ref_meterpoints mp
             inner join ref_meterpoints_attributes mpa on mp.account_id = mpa.account_id
             inner join ref_tariff_history rth on mpa.account_id = rth.account_id
             inner join ref_tariff_history_gas_ur rthe on rth.account_id = rthe.account_id
             inner join ref_tariff_history_gas_sc sthe on rth.account_id = sthe.account_id
             inner join vw_acl_reg_gas_happy vreh on mp.account_id = vreh.account_id
      where mpa.attributes_attributename = 'GSP'
        and (mp.supplyenddate is null or mp.supplyenddate > '2020-03-31')
        and rth.end_date is null
        and rthe.end_date is null
        and sthe.end_date is null
        and mp.meterpointtype = 'G'
      group by rth.tariff_name,
               rth.tariff_type,
               mpa.attributes_attributevalue,
               sthe.rate,
               rthe.rate)
order by 3, 5;