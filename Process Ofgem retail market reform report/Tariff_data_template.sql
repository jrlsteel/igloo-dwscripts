
; with cte_report_dates as
      (
        select
              date
            , substring(date, 9, 10) + '-' + substring(date, 6, 2) + '-' + substring(date, 1, 4) as Report_Date
            , substring(dateadd(day, -1, date::timestamp), 1, 10) as Report_Filter
        from ref_date
        where
            month_name in ('January', 'April', 'July', 'October')
        and day = 1
        and year > 2016
        and date <= substring(sysdate, 1, 10)
      )



, cte_gsp as (

    select
         date,
         name,
         fuel_type ,
         fuel_type_i  ,
          gsp_ldz,
          unit_rate
     from
              (
               select
                 date,
                 name,
                 fuel_type ,
                 fuel_type_i  ,
                  gsp_ldz,
                  unit_rate,
                  cnt,
                  Row_Number() OVER (PARTITION BY date,
                                                  name,
                                                  fuel_type ,
                                                  gsp_ldz ORDER BY cnt desc) rnk
                 from
                       (
                        SELECT
                          crd.date,
                          rt.name,
                          rt.fuel_type ,
                          CASE
                               WHEN rt.fuel_type = 'E' then '1-Elec'
                               WHEN rt.fuel_type = 'G' then '1-Gas'
                           END as  fuel_type_i  ,
                          rt.gsp_ldz,
                          rt.unit_rate,
                         count(rt.unit_rate) as cnt
                        FROM public.ref_tariffs rt
                        right join cte_report_dates crd on crd.date between substring(least(rt.signup_start_date, rt.billing_start_date), 1, 10)
                                                    and substring(nvl(rt.end_date, sysdate), 1, 10)

                        group by
                          crd.date,
                          rt.name,
                          rt.fuel_type,
                          rt.gsp_ldz,
                          rt.unit_rate
                       ) stage1
                   ) stage2
       WHERE rnk = 1
       order by date::timestamp, name, fuel_type, gsp_ldz

       )




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
             crd.Report_Date                  as date,
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
             'U'                              as meter_type,
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

             ---sthe.rate                        as standing_charge,
             19.841                      as standing_charge,
      /*
             ---rthe.rate                        as single_rate_unit_rate,
              CASE
                  WHEN   mpa.attributes_attributevalue = '_A'  THEN  12.393
                  WHEN   mpa.attributes_attributevalue = '_B'   THEN  12.064
                  WHEN   mpa.attributes_attributevalue = '_C'    THEN  11.97
                  WHEN   mpa.attributes_attributevalue = '_D'   THEN  13.192
                  WHEN   mpa.attributes_attributevalue = '_E'   THEN  12.737
                  WHEN   mpa.attributes_attributevalue = '_F'   THEN  12.427
                  WHEN   mpa.attributes_attributevalue = '_G'   THEN  12.609
                  WHEN   mpa.attributes_attributevalue = '_J'  THEN  12.861
                  WHEN   mpa.attributes_attributevalue = '_N'  THEN  12.544
                  WHEN   mpa.attributes_attributevalue = '_K'   THEN  12.961
                  WHEN   mpa.attributes_attributevalue = '_L'   THEN  13.522
                  WHEN   mpa.attributes_attributevalue = '_H'  THEN  12.564
                  WHEN   mpa.attributes_attributevalue = '_M'   THEN  12.233
                  ELSE 0.0
              END                                as single_rate_unit_rate,
       */
              gsp.unit_rate                                as single_rate_unit_rate,
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
             --- inner join vw_acl_reg_elec_happy vreh on mp.account_id = vreh.account_id

             right join cte_report_dates crd on crd.date between substring(greatest(supplystartdate, associationstartdate), 1, 10)
                            and substring(nvl(least(supplyenddate, associationenddate), sysdate), 1, 10)

             left join cte_gsp gsp
                        on gsp.name = rth.tariff_name
                       and gsp.date = crd.date
                       and gsp.fuel_type  = 'E'
                       and gsp.gsp_ldz = mpa.attributes_attributevalue

      where mpa.attributes_attributename = 'GSP'
        and mpa2.attributes_attributename = 'Profile Class'
        --and (mp.supplyenddate is null or mp.supplyenddate > '2020-03-31')
        and rth.end_date is null
        and rthe.end_date is null
        and sthe.end_date is null
        and mp.meterpointtype = 'E'
      group by
               crd.Report_Date,
               rth.tariff_name,
               --- rth.tariff_type,
               mpa.attributes_attributevalue,
               gsp.unit_rate
               ---- sthe.rate,
               ---- rthe.rate

      union

      select 'Igloo Energy'                   as supplier_name,
             crd.Report_Date                        as date,
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

             --- sthe.rate                        as standing_charge,
             23.333                      as standing_charge,

      /*
             ---rthe.rate                        as single_rate_unit_rate,
             CASE
                  WHEN   mpa.attributes_attributevalue = '_A'   THEN  2.765
                  WHEN   mpa.attributes_attributevalue = '_B'   THEN  2.743
                  WHEN   mpa.attributes_attributevalue = '_C'   THEN  2.891
                  WHEN   mpa.attributes_attributevalue = '_D'   THEN  2.842
                  WHEN   mpa.attributes_attributevalue = '_E'  THEN  2.778
                  WHEN   mpa.attributes_attributevalue = '_F'   THEN  2.788
                  WHEN   mpa.attributes_attributevalue = '_G'   THEN  2.81
                  WHEN   mpa.attributes_attributevalue = '_J'  THEN  2.888
                  WHEN   mpa.attributes_attributevalue = '_N'   THEN  2.854
                  WHEN   mpa.attributes_attributevalue = '_K'   THEN  2.817
                  WHEN   mpa.attributes_attributevalue = '_L'   THEN  2.911
                  WHEN   mpa.attributes_attributevalue = '_H'   THEN  2.838
                  WHEN   mpa.attributes_attributevalue = '_M'   THEN  2.819
                  ELSE 0.0
             END                          as single_rate_unit_rate,
        */
              gsp.unit_rate                                as single_rate_unit_rate,

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
            '26/07/2017' as	tariff_offer_date,
            null as	tariff_withdraw_date,
            null as	tariff_expiry_date,
            '01/06/2019' as 	tariff_change_date

      FROM ref_meterpoints mp
             inner join ref_meterpoints_attributes mpa on mp.account_id = mpa.account_id
             inner join ref_tariff_history rth on mpa.account_id = rth.account_id
             inner join ref_tariff_history_gas_ur rthe on rth.account_id = rthe.account_id
             inner join ref_tariff_history_gas_sc sthe on rth.account_id = sthe.account_id
             --- inner join vw_acl_reg_gas_happy vreh on mp.account_id = vreh.account_id

            right join cte_report_dates crd on crd.date between substring(greatest(supplystartdate, associationstartdate), 1, 10)
                            and substring(nvl(least(supplyenddate, associationenddate), sysdate), 1, 10)

            left join cte_gsp gsp
                        on gsp.name = rth.tariff_name
                       and gsp.date = crd.date
                       and gsp.fuel_type  = 'G'
                       and gsp.gsp_ldz = mpa.attributes_attributevalue

      where mpa.attributes_attributename = 'GSP'
        --and (mp.supplyenddate is null or mp.supplyenddate > '2020-03-31')
        and rth.end_date is null
        and rthe.end_date is null
        and sthe.end_date is null
        and mp.meterpointtype = 'G'
      group by
               crd.Report_Date,
               rth.tariff_name,
               -- rth.tariff_type,
               mpa.attributes_attributevalue,
               gsp.unit_rate
               --- sthe.rate,
               --- rthe.rate
     ) stg
order by date::timestamp ,
         tariff_advertised_name ,
         tariff_uid ,
         region
;





