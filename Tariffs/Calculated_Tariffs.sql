drop table if exists temp_calc_tariff_accounts;
create table temp_calc_tariff_accounts as
select cf.account_id,
       acc_tariffs.id                                                                        as tariff_id,
       date_trunc('day', case
                             when cf.home_move_in = false and
                                  cf.signup_date between acc_tariffs.signup_start_date and acc_tariffs.billing_start_date
                                 then cf.fuel_start
                             else greatest(cf.fuel_start, acc_tariffs.billing_start_date)
           end)                                                                              as start_date,
       date_trunc('day', nullif(least(cf.fuel_end, acc_tariffs.end_date), getdate() + 1000)) as end_date
from (select account_id,
             nvl(signup_date, elec_ssd)       as signup_date,
             home_move_in or occupier_account as home_move_in,
             'E'                              as fuel,
             elec_ssd                         as fuel_start,
             nvl(elec_ed, getdate() + 1000)   as fuel_end
      from ref_calculated_daily_customer_file
      where nvl(elec_reg_status, '') not in ('', 'Cancelled')
      union
      select account_id,
             nvl(signup_date, elec_ssd, gas_ssd) as signup_date,
             home_move_in or occupier_account    as home_move_in,
             'G'                                 as fuel,
             gas_ssd                             as fuel_start,
             nvl(gas_ed, getdate() + 1000)       as fuel_end
      from ref_calculated_daily_customer_file
      where nvl(gas_reg_status, '') not in ('', 'Cancelled')) cf
         --- Get the GSP of the account. To include info for gas-only customers (gsp is an elec piece of data) this is done
         --- by linking the account to any other account which has been assigned this meterpoint, then finding any elec
         --- meterpoint associated with any of those linked accounts, then finding the gsp of any of those
         left join (select mp.account_id,
                           max(rma_gsp.attributes_attributevalue) as gsp
                    from ref_meterpoints_raw mp
                             inner join ref_meterpoints_raw prev_at_address
                                        on mp.meter_point_id = prev_at_address.meter_point_id
                             inner join ref_meterpoints_raw address_linked_elec
                                        on prev_at_address.account_id = address_linked_elec.account_id and
                                           address_linked_elec.meterpointtype = 'E'
                             inner join ref_meterpoints_attributes rma_gsp
                                        on rma_gsp.meter_point_id = address_linked_elec.meter_point_id and
                                           rma_gsp.attributes_attributename ilike 'gsp'
                    group by mp.account_id) gsp_link on gsp_link.account_id = cf.account_id
    --- Join the actual tariff info
         left join (select *,
                           nvl(lead(signup_start_date)
                               over (partition by gsp_ldz, fuel_type order by billing_start_date),
                               getdate() + 1000)           as signup_end_date,
                           nvl(end_date, getdate() + 1000) as billing_end_date
                    from ref_tariffs) acc_tariffs
                   on gsp_link.gsp = acc_tariffs.gsp_ldz and acc_tariffs.fuel_type = cf.fuel and
                      (-- Tariff timescale matching
                              (-- Starting tariff
                                      (-- Home move-ins
                                              cf.home_move_in and
                                              cf.fuel_start between acc_tariffs.billing_start_date and acc_tariffs.billing_end_date
                                          ) or
                                      (-- COS gains
                                              cf.home_move_in = false and
                                              ((-- signup during tariff transition window
                                                       cf.signup_date between acc_tariffs.signup_start_date and acc_tariffs.billing_start_date and
                                                       cf.fuel_start < acc_tariffs.billing_end_date) or
                                               (-- signup outside of tariff transition window
                                                       cf.fuel_start between acc_tariffs.billing_start_date and acc_tariffs.billing_end_date and
                                                       cf.signup_date < acc_tariffs.signup_end_date
                                                   )))) or
                              (-- tariff that started while customer was on supply
                                  acc_tariffs.billing_start_date between cf.fuel_start and cf.fuel_end
                                  ))
order by cf.account_id, start_date