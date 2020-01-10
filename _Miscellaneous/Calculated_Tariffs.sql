select cf.account_id,
       acc_tariffs.id                                                         as tariff_id,
       date_trunc('day', case
                             when wl0_date >= acc_tariffs.signup_start_date
                                 then case when acc_tariffs.fuel_type = 'E' then elec_ssd else gas_ssd end
                             else case
                                      when acc_tariffs.fuel_type = 'E'
                                          then greatest(elec_ssd, acc_tariffs.billing_start_date)
                                      else greatest(gas_ssd, acc_tariffs.billing_start_date)
                                 end
           end)                                                               as start_date,
       date_trunc('day', case
                             when acc_tariffs.fuel_type = 'E'
                                 then least(cf.elec_ed, acc_tariffs.end_date)
                             else least(cf.gas_ed, acc_tariffs.end_date) end) as end_date
from (select *, COALESCE(wl0_date, signup_date, gas_ssd, elec_ssd) as agg_signup_date
      from ref_calculated_daily_customer_file) cf
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
    --- Check if the customer signed up in the a new tariff transition period
         left join ref_tariffs starting_tariff on cf.wl0_date >= starting_tariff.signup_start_date and
                                                  cf.wl0_date <= starting_tariff.billing_start_date and
                                                  cf.gsp = starting_tariff.gsp_ldz and
                                                  starting_tariff.fuel_type = 'E'
    --- Join the actual tariff info
         left join ref_tariffs acc_tariffs on gsp_link.gsp = acc_tariffs.gsp_ldz and
    --- Elec tariffs
                                              ((cf.supply_type in ('Elec', 'Dual') and acc_tariffs.fuel_type = 'E' and
                                                  --- Case 1 --- any tariffs which begun while the customer had an elec supply ---
                                                ((acc_tariffs.billing_start_date between cf.agg_signup_date and nvl(cf.elec_ed, '2100-01-01'))
                                                    or
                                                    --- Case 2 --- any tariffs which ended while the customer had an elec supply, except where the customer signed up during a transition period
                                                 (starting_tariff.id is null and
                                                  nvl(acc_tariffs.end_date, '2099-12-31') between cf.agg_signup_date and nvl(cf.elec_ed, '2100-01-01'))
                                                    or
                                                    --- Case 3 --- the tariff which was active when the customer left supply -----
                                                 (starting_tariff.id is null and
                                                  nvl(cf.elec_ed, '2100-01-01') between acc_tariffs.billing_start_date and nvl(acc_tariffs.end_date, '2099-12-31')
                                                     )))
                                                  or
                                                  --- Gas tariffs
                                               (cf.supply_type in ('Gas', 'Dual') and acc_tariffs.fuel_type = 'G' and
                                                   --- Case 1 --- any tariffs which begun while the customer had a gas supply ---
                                                ((acc_tariffs.billing_start_date between cf.agg_signup_date and nvl(cf.gas_ed, '2100-01-01'))
                                                    or
                                                    --- Case 2 --- any tariffs which ended while the customer had an gas supply, except where the customer signed up during a transition period
                                                 (starting_tariff.id is null and
                                                  nvl(acc_tariffs.end_date, '2099-12-31') between cf.agg_signup_date and nvl(cf.gas_ed, '2100-01-01'))
                                                    or
                                                    --- Case 3 --- the tariff which was active when the customer left supply -----
                                                 (starting_tariff.id is null and
                                                  nvl(cf.gas_ed, '2100-01-01') between acc_tariffs.billing_start_date and nvl(acc_tariffs.end_date, '2099-12-31')
                                                     )
                                                    )))