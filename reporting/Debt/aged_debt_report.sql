with cte_constants as (
        select
        14::INT                                                                         as DAY_ZERO_OFFSET
    ),
    cte_payment_day as (
        select all_ids.igl_acc_id,
               sub.day_of_month,
               row_number() over (partition by igl_acc_id) as rownum
        from vw_gocardless_customer_id_mapping all_ids
              left join public.ref_fin_gocardless_mandates man
                    on all_ids.client_id = man.customerid and
                       man.status in ('active', 'submitted')
              left join public.ref_fin_gocardless_subscriptions sub
                    on sub.mandate = man.mandate_id and
                       sub.status = 'active'
    ),
    cte_debt_ages as (
        select vbs.contract_id                                                          as contract_id,
               nvl(sum(case when bill_age <= DAY_ZERO_OFFSET
                        then outstanding_value
                   end), 0)/100::float                                                  as not_overdue,
               nvl(sum(case when bill_age between (1 + DAY_ZERO_OFFSET)
                                      and (29 + DAY_ZERO_OFFSET)
                        then outstanding_value
                   end), 0)/100::float                                                  as from0to29days,
               nvl(sum(case when bill_age between (30 + DAY_ZERO_OFFSET)
                                      and (89 + DAY_ZERO_OFFSET)
                        then outstanding_value
                   end), 0)/100::float                                                  as from30to89days,
               nvl(sum(case when bill_age between (90 + DAY_ZERO_OFFSET)
                                      and (179 + DAY_ZERO_OFFSET)
                        then outstanding_value
                   end), 0)/100::float                                                  as from90to179days,
               nvl(sum(case when bill_age between (180 + DAY_ZERO_OFFSET)
                                      and (364 + DAY_ZERO_OFFSET)
                        then outstanding_value
                   end), 0) /100::float                                                 as from180to364days,
               nvl(sum(case when bill_age > 365 + DAY_ZERO_OFFSET
                        then outstanding_value
                   end), 0)/100::float                                                  as over365days,
               nvl(max(case when outstanding_value > 0
                        and bill_age > DAY_ZERO_OFFSET
                        then bill_age - DAY_ZERO_OFFSET
                        else 0
                   end), 0)::int                                                        as debt_age,
               nvl(max(case when outstanding_value > 0
                        and adjusted_bill_age > DAY_ZERO_OFFSET
                        then adjusted_bill_age - DAY_ZERO_OFFSET
                        else 0
                   end), 0)::int                                                        as adjusted_debt_age
        from vw_debit_status as vbs
                cross join cte_constants constants
        group by contract_id
    )
select dcf.account_id                                                           as account_id,
       dcf.account_status                                                       as account_status,
       dcf.account_loss_type                                                    as account_loss_type,
       dcf.supply_type                                                          as supply_type,
       dcf.gsp                                                                  as gsp,
       dcf.home_move_in                                                         as home_move_in,
       dcf.signup_channel                                                       as signup_channel,
       dcf.first_payment_success                                                as first_payment_success,
       dcf.payment_in_last_month                                                as payment_in_last_month,
       debt_status.payment_method                                               as payment_method,
       payment_day.day_of_month                                                 as payment_day,
       dcf.occupier_account                                                     as occupier_account,
       dcf.balance                                                              as balance,
       case
            when balance  > 0 then 'Debt'
            when balance = 0 then 'Zero'
            when balance < 0 then 'Credit'
       end                                                                      as debt_credit,
       nvl(debt_ages.not_overdue, 0)                                            as not_overdue,
       nvl(debt_ages.from0to29days, 0)                                          as from0to29days,
       nvl(debt_ages.from30to89days, 0)                                         as from30to89days,
       nvl(debt_ages.from90to179days, 0)                                        as from90to179days,
       nvl(debt_ages.from180to364days, 0)                                       as from180to364days,
       nvl(debt_ages.over365days, 0)                                            as over365days,
       nvl(debt_ages.debt_age, 0)                                               as age_of_debt,
       nvl(debt_ages.adjusted_debt_age, 0)                                      as adjusted_debt_age,
       cashflow.most_recent_pa                                                  as pa_last_review_date,
       cashflow.pa_state                                                        as pa_last_review_outcome,
       cashflow.next_expected_pa                                                as pa_next_review_date,
       cashflow.reg_pay_amount                                                  as current_dd,
       cashflow.ideal_dd_now                                                    as ideal_dd,
       cashflow.new_pa_status                                                   as new_pa_status
from cte_debt_ages debt_ages
        left join ref_calculated_daily_customer_file as dcf
            on debt_ages.contract_id = dcf.account_id
        left join ref_account_debt_status debt_status
            on dcf.account_id = debt_status.contract_id
        left join cte_payment_day payment_day
            on dcf.account_id = payment_day.igl_acc_id and
               payment_day.rownum = 1 -- for a small number of accounts, more than one row is returned
        left join vw_pa_cashflow_modelling as cashflow
                on cashflow.ensek_id = debt_ages.contract_id




