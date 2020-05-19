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





      select 'Igloo Energy'                  as supplier_name,
              crd.Report_Date                       as date,
             '1-Gas'                         as gas_tariff_uid_1,
              null                           as gas_tariff_uid_2,
              null                           as gas_tariff_uid_3,
              null                           as gas_tariff_uid_4,
              null                           as gas_tariff_uid_5,
             '1-Elec'                        as electricity_tariff_uid_1,
              null                           as electricity_tariff_uid_2,
              null                           as electricity_tariff_uid_3,
              null                           as electricity_tariff_uid_4,
              null                           as electricity_tariff_uid_5,
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
            end                         as region,

             'D'                              as payment_method_gas_1,
             null                              as payment_method_gas_2,
             null                              as payment_method_gas_3,
             null                              as payment_method_gas_4,
             null                              as payment_method_gas_5,

             'D'                               as payment_method_electricity_1,
             null                               as payment_method_electricity_2,
             null                               as payment_method_electricity_3,
             null                               as payment_method_electricity_4,
             null                               as payment_method_electricity_5,

             'N'                                as  default_3_years_gas_1,
              null                                as  default_3_years_gas_2,
              null                                as  default_3_years_gas_3,
              null                                as  default_3_years_gas_4,
              null                                as  default_3_years_gas_5,

             'N'                                as  default_3_years_electricity_1,
              null                                as  default_3_years_electricity_2,
              null                                as  default_3_years_electricity_3,
              null                                as  default_3_years_electricity_4,
              null                                as  default_3_years_electricity_5,

             count(distinct(ta.account_id))      as number_accounts

      from ref_calculated_tariff_accounts  ta
             inner join ref_tariffs t
              on ta.tariff_id = t.id

            right join cte_report_dates crd on crd.date between substring( t.billing_start_date , 1, 10)
                            and substring(nvl( t.end_date, sysdate), 1, 10)


      group by  crd.Report_Date, t.gsp_ldz
      order by crd.Report_Date::timestamp, t.gsp_ldz
      ;