;
with cte_report_dates as
    (
        select date
             , substring(date, 9, 10) + '-' + substring(date, 6, 2) + '-' + substring(date, 1, 4) as Report_Date
             , substring(dateadd(day, -1, date::timestamp), 1, 10)                                as Report_Filter
        from ref_date
        where month_name in ('January', 'April', 'July', 'October')
          and day = 1
          and year > 2016
          and date <= substring(sysdate, 1, 10)
    )


   , cte_gsp as (
    select date,
           name,
           fuel_type,
           fuel_type_i,
           gsp_ldz,
           unit_rate
    from (
             select date,
                    name,
                    fuel_type,
                    fuel_type_i,
                    gsp_ldz,
                    unit_rate,
                    cnt,
                    Row_Number() OVER (PARTITION BY date,
                        name,
                        fuel_type ,
                        gsp_ldz ORDER BY cnt desc) rnk
             from (
                      SELECT crd.date,
                             rt.name,
                             rt.fuel_type,
                             CASE
                                 WHEN rt.fuel_type = 'E' then '1-Elec'
                                 WHEN rt.fuel_type = 'G' then '1-Gas'
                                 END             as fuel_type_i,
                             rt.gsp_ldz,
                             rt.unit_rate,
                             count(rt.unit_rate) as cnt
                      FROM public.ref_tariffs rt
                               right join cte_report_dates crd on crd.date between substring(
                              least(rt.signup_start_date, rt.billing_start_date), 1, 10)
                          and substring(nvl(rt.end_date, sysdate), 1, 10)

                      group by crd.date,
                               rt.name,
                               rt.fuel_type,
                               rt.gsp_ldz,
                               rt.unit_rate
                  ) stage1
         ) stage2
    WHERE rnk = 1
    order by date::timestamp, name, fuel_type, gsp_ldz
)


   , cte_tariff_change_date as (
    select stg.*,
           substring(start_date, 9, 2) + '-' + substring(start_date, 6, 2) + '-' +
           substring(start_date, 1, 4) as tariff_change_date
    from (
             select tariff_id,
                    min(start_date) as start_date
             FROM ref_calculated_tariff_accounts ta
             group by tariff_id
         ) stg
    order by 1, 2
)


   , cte_tariff_offer_date as (
    select stg.*,
           substring(signup_start_date, 9, 2) + '-' + substring(signup_start_date, 6, 2) + '-' +
           substring(signup_start_date, 1, 4) as tariff_change_date
    from (
             select fuel_type,
                    --gsp_ldz,
                    name,
                    min(billing_start_date) as billing_start_date,
                    min(signup_start_date)  as signup_start_date
             FROM ref_tariffs t
             group by fuel_type,
                      --gsp_ldz,
                      name
         ) stg
    order by 1
)


select distinct supplier_name,
                date,
                tariff_uid,
                tariff_advertised_name,
                region,
                meter_type,
                tariff_type,
                tariff_fuel_type,
                payment_method,
                online_account,
                paperless_billing,
                renewable_percentage,
                default_3_years,
                number_of_customer_accounts,
                'N'  as is_tou_tariff,
                is_multi_reg_tariff,
                is_multi_tier_tariff,
                standing_charge,
                single_rate_unit_rate,
                null as multi_tier_volume_break_1,
                null as multi_tier_volume_break_2,
                null as multi_tier_volume_break_3,
                null as multi_tier_volume_break_4,
                null as multi_tier_volume_break_1_uom,
                null as multi_tier_volume_break_2_uom,
                null as multi_tier_volume_break_3_uom,
                null as multi_tier_volume_break_4_uom,
                null as multi_tier_unit_rate_1,
                null as multi_tier_unit_rate_2,
                null as multi_tier_unit_rate_3,
                null as multi_tier_unit_rate_4,
                null as multi_tier_unit_rate_5,
                null as multi_tier_unit_rate_op,
                null as multi_tier_unit_rate_op_2,
                null as multi_tier_unit_rate_op_3,
                null as assumed_consumption_split_1,
                null as assumed_consumption_split_2,
                null as assumed_consumption_split_3,
                null as assumed_consumption_split_4,
                null as assumed_consumption_split_5,
                null as multi_reg_period_1_unit_rate,
                null as multi_reg_period_2_unit_rate,
                null as multi_reg_period_3_unit_rate,
                null as multi_reg_period_4_unit_rate,
                null as multi_reg_period_5_unit_rate,
                null as dual_fuel_discount,
                null as online_discount,
                termination_fee,
                fix_length,
                tariff_offer_date,
                tariff_withdraw_date,
                tariff_expiry_date,
                tariff_change_date

from (select 'Igloo Energy'                  as supplier_name,
             crd.Report_Date                 as date,
             '1-Elec'                        as tariff_uid,
             t.name                             tariff_advertised_name,
             case
                 when t.gsp_ldz = '_A' then 'east_england'
                 when t.gsp_ldz = '_B' then 'east_midlands'
                 when t.gsp_ldz = '_C' then 'london'
                 when t.gsp_ldz = '_D' then 'merseyside_and_north_wales'
                 when t.gsp_ldz = '_E' then 'midlands'
                 when t.gsp_ldz = '_F' then 'north_east'
                 when t.gsp_ldz = '_G' then 'north_west'
                 when t.gsp_ldz = '_H' then 'southern'
                 when t.gsp_ldz = '_J' then 'south_east'
                 when t.gsp_ldz = '_K' then 'south_wales'
                 when t.gsp_ldz = '_L' then 'south_west'
                 when t.gsp_ldz = '_M' then 'yorkshire'
                 when t.gsp_ldz = '_N' then 'south_scotland'
                 when t.gsp_ldz = '_P' then 'north_scotland'
                 else t.gsp_ldz
                 end                         as region,
             'U'                             as meter_type,
             'S'                             as tariff_type,
             'E'                             as tariff_fuel_type,
             'D'                             as payment_method,
             'Y'                             as online_account,
             'Y'                             as paperless_billing,
             2.6                             as renewable_percentage,
             'N'                             as default_3_years,
             count(distinct (ta.account_id)) as number_of_customer_accounts,
             'N'                             as is_multi_reg_tariff,
             'N'                             as is_multi_tier_tariff,
             t.standing_charge               as standing_charge,
             t.unit_rate                     as single_rate_unit_rate,
             0                               as termination_fee,
             null                            as fix_length,
             '31/03/2017'                    as tariff_offer_date,
             null                            as tariff_withdraw_date,
             null                            as tariff_expiry_date,
             t.billing_start_date            as tariff_change_date

      FROM ref_calculated_tariff_accounts ta
               inner join ref_tariffs t
                          on ta.tariff_id = t.id
                              and t.fuel_type = 'E'

               right join cte_report_dates crd on crd.date between substring(ta.start_date, 1, 10)
          and substring(nvl(ta.end_date, sysdate), 1, 10)

               left join cte_tariff_offer_date tod
                         on tod.fuel_type = t.fuel_type
                             and tod.name = t.name


      group by crd.Report_Date,
               t.name,
               t.gsp_ldz,
               t.unit_rate,
               t.standing_charge,
               t.billing_start_date

      union

      select 'Igloo Energy'                  as supplier_name,
             crd.Report_Date                 as date,
             '1-Gas'                         as tariff_uid,
             t.name                             tariff_advertised_name,
             case
                 when t.gsp_ldz = '_A' then 'east_england'
                 when t.gsp_ldz = '_B' then 'east_midlands'
                 when t.gsp_ldz = '_C' then 'london'
                 when t.gsp_ldz = '_D' then 'merseyside_and_north_wales'
                 when t.gsp_ldz = '_E' then 'midlands'
                 when t.gsp_ldz = '_F' then 'north_east'
                 when t.gsp_ldz = '_G' then 'north_west'
                 when t.gsp_ldz = '_H' then 'southern'
                 when t.gsp_ldz = '_J' then 'south_east'
                 when t.gsp_ldz = '_K' then 'south_wales'
                 when t.gsp_ldz = '_L' then 'south_west'
                 when t.gsp_ldz = '_M' then 'yorkshire'
                 when t.gsp_ldz = '_N' then 'south_scotland'
                 when t.gsp_ldz = '_P' then 'north_scotland'
                 else t.gsp_ldz
                 end                         as region,
             'U'                             as meter_type,
             'S'                                tariff_type,
             'G'                             as tariff_fuel_type,
             'D'                             as payment_method,
             'Y'                             as online_account,
             'Y'                             as paperless_billing,
             0                               as renewable_percentage,
             'N'                             as default_3_years,
             count(distinct (ta.account_id)) as number_of_customer_accounts,
             'N'                             as is_multi_reg_tariff,
             'N'                             as is_multi_tier_tariff,
             t.standing_charge               as standing_charge,
             t.unit_rate                     as single_rate_unit_rate,
             0                               as termination_fee,
             null                            as fix_length,
             '26/07/2017'                    as tariff_offer_date,
             null                            as tariff_withdraw_date,
             null                            as tariff_expiry_date,
             t.billing_start_date            as tariff_change_date

      FROM ref_calculated_tariff_accounts ta
               inner join ref_tariffs t
                          on ta.tariff_id = t.id
                              and t.fuel_type = 'G'

               right join cte_report_dates crd on crd.date between substring(ta.start_date, 1, 10)
          and substring(nvl(ta.end_date, sysdate), 1, 10)

      group by crd.Report_Date,
               t.name,
               t.gsp_ldz,
               t.unit_rate,
               t.standing_charge,
               t.billing_start_date
     ) stg

where date = '01-10-2020'

order by date::timestamp,
         tariff_advertised_name,
         tariff_uid,
         region
;





