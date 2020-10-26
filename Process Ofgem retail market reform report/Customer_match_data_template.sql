;
with cte_report_dates as
    (
        select date as raw_date
        from ref_date
        where month_name in ('January', 'April', 'July', 'October')
          and day = 1
          and year >= 2017
          and date <= getdate()
    )
   , cte_tariff_accounts_full as (
    select ta.account_id,
           ta.start_date,
           ta.end_date,
           t.fuel_type,
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
               end as region
    from ref_calculated_tariff_accounts ta
             inner join ref_tariffs t on ta.tariff_id = t.id
)
   , cte_supply_type_counts_at_report_date as (
    select crd.raw_date,
           count(distinct nvl(tariff_elec.account_id, tariff_gas.account_id)),
           tariff_elec.region as elec_region,
           tariff_gas.region  as gas_region
    from cte_report_dates crd
             left join cte_tariff_accounts_full tariff_elec
                       on crd.raw_date between tariff_elec.start_date and nvl(tariff_elec.end_date, getdate() + 100) and
                          tariff_elec.fuel_type = 'E'
             left join cte_tariff_accounts_full tariff_gas
                       on (tariff_elec.account_id is null or tariff_elec.account_id = tariff_gas.account_id) and
                          crd.raw_date between tariff_gas.start_date and nvl(tariff_gas.end_date, getdate() + 100) and
                          tariff_gas.fuel_type = 'G'
    group by raw_date, elec_region, gas_region
)

select 'Igloo Energy'                                      as supplier_name,
       to_char(raw_date, 'DD-MM-YYYY')                     as date,
       case when gas_region is not null then '1-Gas' end   as gas_tariff_uid_1,
       null                                                as gas_tariff_uid_2,
       null                                                as gas_tariff_uid_3,
       null                                                as gas_tariff_uid_4,
       null                                                as gas_tariff_uid_5,
       case when elec_region is not null then '1-Elec' end as electricity_tariff_uid_1,
       null                                                as electricity_tariff_uid_2,
       null                                                as electricity_tariff_uid_3,
       null                                                as electricity_tariff_uid_4,
       null                                                as electricity_tariff_uid_5,
       elec_region                                         as electricity_region,
       gas_region                                          as gas_region,

       'D'                                                 as payment_method_gas_1,
       null                                                as payment_method_gas_2,
       null                                                as payment_method_gas_3,
       null                                                as payment_method_gas_4,
       null                                                as payment_method_gas_5,

       'D'                                                 as payment_method_electricity_1,
       null                                                as payment_method_electricity_2,
       null                                                as payment_method_electricity_3,
       null                                                as payment_method_electricity_4,
       null                                                as payment_method_electricity_5,

       'N'                                                 as default_3_years_gas_1,
       null                                                as default_3_years_gas_2,
       null                                                as default_3_years_gas_3,
       null                                                as default_3_years_gas_4,
       null                                                as default_3_years_gas_5,

       'N'                                                 as default_3_years_electricity_1,
       null                                                as default_3_years_electricity_2,
       null                                                as default_3_years_electricity_3,
       null                                                as default_3_years_electricity_4,
       null                                                as default_3_years_electricity_5,

       count(*)                                            as number_accounts

from (select crd.raw_date,
             ta.account_id,
             max(case when fuel_type = 'E' then region end) as elec_region,
             max(case when fuel_type = 'G' then region end) as gas_region
      from cte_report_dates crd
               left join cte_tariff_accounts_full ta
                         on crd.raw_date between ta.start_date and nvl(ta.end_date, getdate() + 100)
      group by raw_date, account_id) account_report_level

     -- where date = '01-10-2020'

group by raw_date, elec_region, gas_region
order by raw_date, electricity_tariff_uid_1, gas_tariff_uid_1, elec_region, gas_region
;


with cte_report_dates as (
    select date                        as raw_date,
           to_char(date, 'DD-MM-YYYY') as formatted_date
    from ref_date
    where month_name in ('January', 'April', 'July', 'October')
      and day = 1
      and year >= 2017
      and date <= getdate()
)
   , cte_tariff_accounts_full as (
    select ta.account_id,
           ta.start_date,
           ta.end_date,
           t.fuel_type,
           t.gsp_ldz
    from ref_calculated_tariff_accounts ta
             inner join ref_tariffs t on ta.tariff_id = t.id
)
select raw_date,
       elec_region,
       gas_region,
       count(*) as num_accounts

from (select crd.raw_date,
             ta.account_id,
             max(case when fuel_type = 'E' then gsp_ldz end) as elec_region,
             max(case when fuel_type = 'G' then gsp_ldz end) as gas_region
      from cte_report_dates crd
               left join cte_tariff_accounts_full ta
                         on crd.raw_date between ta.start_date and nvl(ta.end_date, getdate() + 100)
      group by raw_date, account_id) account_report_level
group by raw_date, elec_region, gas_region
order by raw_date, elec_region, gas_region