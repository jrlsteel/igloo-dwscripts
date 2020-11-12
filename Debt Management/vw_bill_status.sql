create or replace view vw_bill_status as
select transaction_id,
       users.id                                                      as user_id,
       account_id                                                    as contract_id,
       'ensek_account'                                               as contract_type,
       creationdetail_createddate::timestamp                         as bill_date,
       datediff(days, bill_date, getdate())                          as bill_age,
       0                                                             as hold_days,
       bill_age - hold_days                                          as adjusted_bill_age,
       amount_pence                                                  as bill_amount,
       debit_new - bill_amount                                       as debit_prior,
       debit_new,
       credit_present,
       least(greatest(credit_present - debit_prior, 0), bill_amount) as value_paid_off,
       bill_amount - value_paid_off                                  as outstanding_value,
       outstanding_value = 0                                         as paid_off
from ref_cdb_users users
         left join ref_cdb_user_permissions up on users.id = up.user_id and
                                                  up.permissionable_type = 'App\\SupplyContract' and
                                                  up.permission_level = 0
         left join ref_cdb_supply_contracts sc on up.permissionable_id = sc.id
         left join (select id                                                       as transaction_id,
                           account_id,
                           creationdetail_createddate,
                           round(amount * 100)::int                                 as amount_pence,
                           transactiontype,
                           case when amount_pence > 0 then amount_pence else 0 end  as debit_amount,
                           case when amount_pence < 0 then -amount_pence else 0 end as credit_amount,
                           sum(debit_amount)
                           over (partition by account_id order by creationdetail_createddate
                               rows between unbounded preceding and current row)    as debit_new,
                           sum(credit_amount) over (partition by account_id)        as credit_present,
                           currentbalance                                           as ensek_calculated_balance
                    from ref_account_transactions) summed_transactions
                   on summed_transactions.account_id = sc.external_id
where transactiontype = 'BILL'
  and amount_pence > 0
order by user_id, contract_id, bill_date;

alter table vw_bill_status
    owner to igloo;