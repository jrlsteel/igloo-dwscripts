      select 'Igloo Energy'                  as supplier_name,
             '01/04/2020'                       as date,
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

             count(distinct(rth.account_id))      as number_accounts
      from ref_meterpoints mp
             inner join ref_meterpoints_attributes mpa on mp.account_id = mpa.account_id
             inner join ref_tariff_history rth on mpa.account_id = rth.account_id
             inner join ref_meterpoints mp1 on mp.account_id = mp1.account_id
             inner join vw_acl_reg_gaselec_happy vreh on mp.account_id = vreh.account_id
      WHERE mp.meterpointtype = 'E'
        and (mp.supplyenddate is null or mp.supplyenddate > '2020-03-31')
        and mp1.meterpointtype = 'G'
        and (mp1.supplyenddate is null or mp1.supplyenddate > '2020-03-31')
        and mpa.attributes_attributename = 'GSP'
      group by mpa.attributes_attributevalue
      ;