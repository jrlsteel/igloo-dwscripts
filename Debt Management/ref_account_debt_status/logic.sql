-- drop table temp_account_debt_status;
-- create table temp_account_debt_status as
select users.id                                                                 as user_id,
       sc.external_id                                                           as contract_id,
       'ensek_account'                                                          as contract_type,
       latest_transaction.currentbalance                                        as current_account_balance,
       current_gocardless.total_monthly_payment                                 as current_direct_debit,
       nvl(oldest_unpaid_bill.bills_outstanding, 0)                             as bills_outstanding,
       oldest_unpaid_bill.transaction_id,
       oldest_unpaid_bill.bill_date,
       oldest_unpaid_bill.bill_age,
       oldest_unpaid_bill.hold_days,
       oldest_unpaid_bill.adjusted_bill_age,
       oldest_unpaid_bill.bill_amount,
       oldest_unpaid_bill.value_paid_off,
       oldest_unpaid_bill.outstanding_value,
       decode(nvl(current_gocardless.active_subscriptions, 0), 0, 'PORB', 'DD') as payment_method
from ref_cdb_users users
         left join ref_cdb_user_permissions up on up.user_id = users.id and
                                                  up.permission_level = 0 and
                                                  up.permissionable_type = 'App\\SupplyContract'
         left join ref_cdb_supply_contracts sc on up.permissionable_id = sc.id
         left join (select *,
                           row_number()
                           over (partition by user_id, contract_id/*, contract_type*/ order by bill_date) as unpaid_bill_number,
                           count(*) over (partition by user_id, contract_id/*, contract_type*/)           as bills_outstanding
                    from vw_bill_status
                    where not paid_off) oldest_unpaid_bill on oldest_unpaid_bill.unpaid_bill_number = 1 and
                                                              oldest_unpaid_bill.user_id = users.id and
                                                              oldest_unpaid_bill.contract_id = sc.external_id and
                                                              oldest_unpaid_bill.contract_type = 'ensek_account'
         left join (select *,
                           row_number() over (partition by account_id order by creationdetail_createddate desc) as rn
                    from ref_account_transactions) latest_transaction on latest_transaction.rn = 1 and
                                                                         latest_transaction.account_id = sc.external_id
         left join (select gc_id_map.ensekid          as ensek_contract_id,
                           count(mandates.mandate_id) as active_mandates,
                           count(subscriptions.id)    as active_subscriptions,
                           sum(subscriptions.amount)  as total_monthly_payment
                    from (select distinct client_id, ensekid
                          from aws_fin_stage1_extracts.fin_go_cardless_api_clients) gc_id_map
                             left join ref_fin_gocardless_mandates mandates
                                       on mandates.customerid = gc_id_map.client_id and mandates.status = 'active'
                             left join ref_fin_gocardless_subscriptions subscriptions
                                       on subscriptions.mandate = mandates.mandate_id and
                                          subscriptions.status = 'active'
                    group by ensek_contract_id) current_gocardless on ensek_contract_id = sc.external_id;