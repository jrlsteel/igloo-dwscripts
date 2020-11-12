select dcf.account_id                                                                                 as ensek_id,
       account_status,
       occupier_account,
       trunc(latest_dd_received_date)                                                                 as latest_dd_received_date,
       round(reg_pay_amount, 2)                                                                       as reg_pay_amount,
       round(reg_pay_amount_ex_wu, 2)                                                                 as reg_pay_amount_ex_wu,
       round(nvl(((num_elec_mpns * vlr.elec_sc * 3.65) + (eac_igloo_ca * vlr.elec_ur * 0.01)) / 12,
                 0) *
             1.05,
             2)                                                                                       as elec_monthly_usage_sterling,
       round(nvl(((num_gas_mpns * vlr.gas_sc * 3.65) + (aq_igloo_ca * vlr.gas_ur * 0.01)) / 12,
                 0) *
             1.05,
             2)                                                                                       as gas_monthly_usage_sterling,
       elec_monthly_usage_sterling + gas_monthly_usage_sterling                                       as total_monthly_usage_sterling,
       balance                                                                                        as account_balance,
       balance > 0                                                                                    as in_debt,
       case when balance > reg_pay_amount then balance / 12 else 0 end                                as monthly_balance_management,
       monthly_bill_date                                                                              as bill_date,
       round(total_monthly_usage_sterling + monthly_balance_management, 2)                            as ideal_dd_now,
       round(ideal_dd_now - reg_pay_amount, 2)                                                        as ideal_dd_diff,
       case
           when ideal_dd_diff > 2.5 and ideal_dd_diff > (reg_pay_amount * 0.05) then 'change_advised'
           else 'no_action' end                                                                       as new_pa_status,
       case when new_pa_status = 'change_advised' then ideal_dd_now else round(reg_pay_amount, 2) end as advised_dd,
       case
           when reg_pay_amount > 0 then
               round((ideal_dd_now - reg_pay_amount) * 100 / reg_pay_amount, 1) end                   as perc_dd_change,
       trunc(case
                 when last_pa.updated_at >= dateadd(months, 6, dcf.acc_ssd)
                     then last_pa.updated_at end)                                                     as most_recent_pa,
       case when most_recent_pa is not null then last_pa.status end                                   as pa_state,
       trunc(case
                 when datediff(months, dcf.acc_ssd, getdate()) < 6 then dateadd(months, 6, dcf.acc_ssd) + 1
                 when pa_state = 'exception' or pa_state is null then
                     dateadd(months,
                             (date_part(day, getdate()) >= dcf.monthly_bill_date)::int, -- add a month if this month's bill date has passed
                             date_trunc('month', getdate()) + dcf.monthly_bill_date)
                 else dateadd(months, 3, most_recent_pa)
           end)                                                                                       as next_expected_PA
from ref_calculated_daily_customer_file dcf
         left join vw_latest_rates vlr on dcf.account_id = vlr.account_id
         left join ref_cdb_supply_contracts sc on dcf.account_id = sc.external_id
         left join temp_pa_outcomes last_pa on last_pa.supply_contract_id = sc.id
order by dcf.account_id