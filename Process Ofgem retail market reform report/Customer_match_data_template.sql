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
               when dcf.gsp = '_A' then 'east_england'
               when dcf.gsp = '_B' then 'east_midlands'
               when dcf.gsp = '_C' then 'london'
               when dcf.gsp = '_D' then 'merseyside_and_north_wales'
               when dcf.gsp = '_E' then 'midlands'
               when dcf.gsp = '_F' then 'north_east'
               when dcf.gsp = '_G' then 'north_west'
               when dcf.gsp = '_H' then 'southern'
               when dcf.gsp = '_J' then 'south_east'
               when dcf.gsp = '_K' then 'south_wales'
               when dcf.gsp = '_L' then 'south_west'
               when dcf.gsp = '_M' then 'yorkshire'
               when dcf.gsp = '_N' then 'south_scotland'
               when dcf.gsp = '_P' then 'north_scotland'
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

             count(distinct(dcf.account_id))      as number_accounts
      from ref_calculated_daily_customer_file dcf
             ---- inner join ref_meterpoints_attributes mpa on mp.account_id = mpa.account_id
             ---- inner join ref_tariff_history rth on mpa.account_id = rth.account_id
             ---- inner join ref_meterpoints mp1 on mp.account_id = mp1.account_id
             ---- inner join vw_acl_reg_gaselec_happy vreh on mp.account_id = vreh.account_id

             right join cte_report_dates crd on crd.date between substring( dcf.acc_ssd , 1, 10)
                            and substring(nvl( dcf.acc_ed, sysdate), 1, 10)


      group by  crd.Report_Date, dcf.gsp
      order by crd.Report_Date::timestamp, dcf.gsp
      ;