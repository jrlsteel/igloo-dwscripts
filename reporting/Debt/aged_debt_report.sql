with cte_constants as (
        select
        14::INT                                                     as DAY_ZERO_OFFSET,
        nvl(null, getdate())                                        as AT_DATE
    ),
    cte_summed_transactions as (
        select id                                                   as transaction_id,
        account_id,
        creationdetail_createddate,
        round(amount * 100)::int                                    as amount_pence,
        transactiontype,
        case when amount_pence > 0 then amount_pence else 0 end     as debit_amount,
        case when amount_pence < 0 then -amount_pence else 0 end    as credit_amount,
        sum(debit_amount)
        over (partition by account_id
              order by creationdetail_createddate
              rows between unbounded preceding and current row)     as debit_new,
        sum(credit_amount) over (partition by account_id)           as credit_present,
        currentbalance                                              as ensek_calculated_balance
        from public.ref_account_transactions
            cross join cte_constants constants
        where creationdetail_createddate < AT_DATE
    ),
    cte_debit_status as (
        with cte_users as (
             select user_id                                          as id,
                   sc.external_id                                    as account_id
                from public.ref_cdb_users users
                     left join public.ref_cdb_user_permissions up on users.id = up.user_id and
                                                              up.permissionable_type = 'App\\SupplyContract' and
                                                              up.permission_level = 0
                     left join public.ref_cdb_supply_contracts sc on up.permissionable_id = sc.id
             )
        select transaction_id,
               users.id                                                      as user_id,
               summed_transactions.account_id                                as contract_id,
               'ensek_account'                                               as contract_type,
               creationdetail_createddate::timestamp                         as bill_date,
               datediff(days, bill_date, AT_DATE)                            as bill_age,
               0                                                             as hold_days,
               bill_age - hold_days                                          as adjusted_bill_age,
               amount_pence                                                  as bill_amount,
               debit_new - bill_amount                                       as debit_prior,
               debit_new,
               credit_present,
               least(greatest(credit_present - debit_prior, 0), bill_amount) as value_paid_off,
               bill_amount - value_paid_off                                  as outstanding_value,
               outstanding_value = 0                                         as paid_off
        from  cte_summed_transactions summed_transactions
             left join cte_users users on users.account_id = summed_transactions.account_id
             cross join cte_constants constants
        where amount_pence > 0
    ),
    cte_payment_day as (
        select all_ids.igl_acc_id,
               sub.day_of_month,
               row_number()
               over (partition by igl_acc_id) as rownum
        from vw_gocardless_customer_id_mapping all_ids
              left join public.ref_fin_gocardless_mandates man
                    on all_ids.client_id = man.customerid and
                       man.status in ('active', 'submitted')
              left join public.ref_fin_gocardless_subscriptions sub
                    on sub.mandate = man.mandate_id and
                       sub.status = 'active'
    ),
    cte_balance_between_dates as (
        select creationdetail_createddate                                               as start_date,
               nvl(lead(creationdetail_createddate, 1)
               over (
                   partition by account_id
                   order by creationdetail_createddate
               ), getdate() + 1)                                                        as end_date,
               currentbalance                                                           as balance,
               account_id
        from public.ref_account_transactions
    ),
    cte_balance_management as (
        select supply_contracts.external_id                                             as account_id,
               payment_layers.effective_from                                            as start_date,
               payment_layers.effective_to                                              as to_date,
               round(payment_layers.amount, 2)                                          as amount,
               datediff(
                    'month',
                    payment_layers.effective_from,
                    payment_layers.effective_to
               )                                                                        as number_of_payments
        from aws_s3_stage2_extracts.stage2_cdbpaymentlayers as payment_layers
             left join aws_s3_stage2_extracts.stage2_cdbpaymenttypes as payment_types
                on payment_types.id = payment_layers.payment_type_id and
                   payment_types.slug = 'debt_recovery'
             left join public.ref_cdb_supply_contracts as supply_contracts
                on supply_contracts.id = payment_layers.supply_contract_id
        where current_date between payment_layers.effective_from and payment_layers.effective_to
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
                                      and (365 + DAY_ZERO_OFFSET)
                        then outstanding_value
                   end), 0) /100::float                                                 as from180to365days,
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
        from cte_debit_status as vbs
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
       case
           when dcf.occupier_account then 'PORB'
           else debt_status.payment_method
       end                                                                      as payment_method,
       payment_day.day_of_month                                                 as payment_day,
       dcf.occupier_account                                                     as occupier_account,
       bal.balance                                                              as balance,
       case
            when bal.balance  > 0 then 'Debt'
            when bal.balance = 0 then 'Zero'
            when bal.balance < 0 then 'Credit'
       end                                                                      as debt_credit,
       nvl(debt_ages.not_overdue, 0)                                            as not_overdue,
       nvl(debt_ages.from0to29days, 0)                                          as from0to29days,
       nvl(debt_ages.from30to89days, 0)                                         as from30to89days,
       nvl(debt_ages.from90to179days, 0)                                        as from90to179days,
       nvl(debt_ages.from180to365days, 0)                                       as from180to365days,
       nvl(debt_ages.over365days, 0)                                            as over365days,
       nvl(debt_ages.debt_age, 0)                                               as age_of_debt,
       nvl(debt_ages.adjusted_debt_age, 0)                                      as adjusted_debt_age,
       cashflow.most_recent_pa                                                  as pa_last_review_date,
       cashflow.pa_state                                                        as pa_last_review_outcome,
       cashflow.next_expected_pa                                                as pa_next_review_date,
       cashflow.reg_pay_amount                                                  as current_dd,
       cashflow.ideal_dd_now                                                    as ideal_dd,
       cashflow.new_pa_status                                                   as new_pa_status,
       balance_management.start_date                                            as bm_start_date,
       balance_management.to_date                                               as bm_to_date,
       balance_management.amount                                                as bm_amount,
       balance_management.number_of_payments                                    as bm_number_of_payments,
       balman_start.balance                                                     as bm_starting_balance
from cte_debt_ages debt_ages
        cross join cte_constants constants
        left join public.ref_calculated_daily_customer_file as dcf
            on debt_ages.contract_id = dcf.account_id
        left join cte_balance_between_dates as bal
            on debt_ages.contract_id = bal.account_id and
               AT_DATE between bal.start_date and bal.end_date
        left join public.ref_account_debt_status debt_status
            on dcf.account_id = debt_status.contract_id
        left join cte_payment_day payment_day
            on dcf.account_id = payment_day.igl_acc_id and
               payment_day.rownum = 1 -- for a small number of accounts, more than one row is returned
        left join public.vw_pa_cashflow_modelling as cashflow
                on cashflow.ensek_id = debt_ages.contract_id
        left join cte_balance_management as balance_management
                on balance_management.account_id = debt_ages.contract_id
        left join cte_balance_between_dates as balman_start
                on balman_start.account_id = balance_management.account_id and
                   balance_management.start_date between balman_start.start_date and balman_start.end_date
