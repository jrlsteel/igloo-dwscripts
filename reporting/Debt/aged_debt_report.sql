with cte_variables as (
        select
        14::INT                                                                         as day_zero_offset
    ),
    cte_debt_ages as (
        select vbs.contract_id                                                          as contract_id,
               nvl(sum(case when bill_age <= day_zero_offset
                        then outstanding_value
                   end), 0)/100::float                                                  as not_overdue,
               nvl(sum(case when bill_age between (1 + day_zero_offset)
                                      and (29 + day_zero_offset)
                        then outstanding_value
                   end), 0)/100::float                                                  as from0to29days,
               nvl(sum(case when bill_age between (30 + day_zero_offset)
                                      and (89 + day_zero_offset)
                        then outstanding_value
                   end), 0)/100::float                                                  as from30to89days,
               nvl(sum(case when bill_age between (90 + day_zero_offset)
                                      and (179 + day_zero_offset)
                        then outstanding_value
                   end), 0)/100::float                                                  as from90to179days,
               nvl(sum(case when bill_age between (180 + day_zero_offset)
                                      and (364 + day_zero_offset)
                        then outstanding_value
                   end), 0) /100::float                                                 as from180to364days,
               nvl(sum(case when bill_age > 365 + day_zero_offset
                        then outstanding_value
                   end), 0)/100::float                                                  as over365days,
               nvl(max(case when outstanding_value > 0
                        and bill_age > day_zero_offset
                        then bill_age - day_zero_offset
                        else 0
                   end), 0)::int                                                        as debt_age,
               nvl(max(case when outstanding_value > 0
                        and adjusted_bill_age > day_zero_offset
                        then adjusted_bill_age - day_zero_offset
                        else 0
                   end), 0)::int                                                        as adjusted_debt_age
        from vw_debit_status as vbs
                cross join cte_variables variables
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
       nvl(debt_ages.adjusted_debt_age, 0)                                      as adjusted_debt_age
from cte_debt_ages debt_ages
        left join ref_calculated_daily_customer_file as dcf
            on debt_ages.contract_id = dcf.account_id
        left join ref_account_debt_status debt_status
                on dcf.account_id = debt_status.contract_id



