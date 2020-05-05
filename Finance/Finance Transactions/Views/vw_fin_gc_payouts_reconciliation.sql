create or replace view vw_fin_gc_payouts_reconciliation as
--- Current Script is for Feb 2020 -------------

with cte_payouts as (
select *,
substring (created_at, 1, 7) as payout_month
from aws_fin_stage1_extracts.fin_go_cardless_api_payouts
where
substring (created_at, 1, 10) between '2017-01-01' and to_char(sysdate, 'YYYY-MM-DD')
)


, cte_events_payout as (
select *,
substring (created_at, 1, 7) as payout_month
from aws_fin_stage1_extracts.fin_go_cardless_api_events events_a
where events_a.resource_type = 'payouts'
and substring (events_a.created_at, 1, 10) between '2017-01-01' and to_char(sysdate, 'YYYY-MM-DD')
)


, cte_payments as (
---- Payments -----

select distinct events.id,
events.created_at as events_created_at,
events.resource_type,
events.action,
events.customer_notifications,
events.cause,
events.description,
events.origin,
events.reason_code,
events.scheme,
events.will_attempt_retry,
events.mandate as events_mandate,
events.new_customer_bank_account,
events.new_mandate,
events.organisation,
events.parent_event,
events.payment,
events.payout,
events.previous_customer_bank_account,
events.refund,
events.subscription,
payments.id as paymentID,
payments.amount ::float / 100 as amount,
payments.amount_refunded ::float / 100 as amount_refunded,
payments.created_at,
payments.charge_date,
payments.status,
man.mandate_id,
payments.description,
payments.reference,
payments.payout,
payouts.payout_id,
events_payout.payout_month,
client.client_id as customers_id,
lkp.igl_acc_id as ensekAccountId
from cte_events_payout events_payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
inner join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on payments.id = events.payment
inner join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts
on payouts.payout_id = events_payout.payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
on man.mandate_id = payments.mandate --- goCardless CLIENTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
on client.client_id = man.customerid --- ref_cdb_users ---
left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'paid_out'
)


, cte_refundsSettled as (
---- Event-Refunds RefundSettled -----

select distinct events.id,
events.created_at as events_created_at,
events.resource_type,
events.action,
events.customer_notifications,
events.cause,
events.description,
events.origin,
events.reason_code,
events.scheme,
events.will_attempt_retry,
events.mandate as events_mandate,
events.new_customer_bank_account,
events.new_mandate,
events.organisation,
events.parent_event,
events.payment,
events.payout,
events.previous_customer_bank_account,
events.refund,
events.subscription,
refunds.id as refundID,
refunds.amount ::float / 100 as amount,
refunds.payment as paymentID,
refunds.created_at as refunds_created_at,
payments.status as payment_status,
payments.payout as payoutID,
payouts.payout_id,
events_payout.payout_month,
man.mandate_id,
client.client_id as customers_id,
lkp.igl_acc_id as ensekAccountId
from cte_events_payout events_payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
inner join aws_fin_stage1_extracts.fin_go_cardless_api_refunds refunds on events.refund = refunds.id
left join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on refunds.payment = payments.id
left join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts on payouts.payout_id = events_payout.payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
on man.mandate_id = refunds.mandate --- goCardless CLIENTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
on client.client_id = man.customerid --- ref_cdb_users ---
left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'refund_settled'
)


, cte_lateFailure as (
---- Event-Payments late_failure_settled-----

select distinct events.id,
events.created_at as events_created_at,
events.resource_type,
events.action,
events.customer_notifications,
events.cause,
events.description,
events.origin,
events.reason_code,
events.scheme,
events.will_attempt_retry,
events.mandate as events_mandate,
events.new_customer_bank_account,
events.new_mandate,
events.organisation,
events.parent_event,
events.payment,
events.payout,
events.previous_customer_bank_account,
events.refund,
events.subscription,
payments.id as paymentID,
payments.amount ::float / 100 as amount,
payments.amount_refunded ::float / 100 as amount_refunded,
payments.created_at,
payments.charge_date,
payments.status,
man.mandate_id,
payments.description,
payments.reference,
payments.payout,
payouts.payout_id,
events_payout.payout_month,
client.client_id as customers_id,
lkp.igl_acc_id as ensekAccountId
from cte_events_payout events_payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
inner join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on payments.id = events.payment
inner join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts
on payouts.payout_id = events_payout.payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
on man.mandate_id = payments.mandate --- goCardless CLIENTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
on client.client_id = man.customerid --- ref_cdb_users ---
left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'late_failure_settled'
)


, cte_chargeBack as (
---- Event-Payments chargeback_settled-----

select distinct events.id,
events.created_at as events_created_at,
events.resource_type,
events.action,
events.customer_notifications,
events.cause,
events.description,
events.origin,
events.reason_code,
events.scheme,
events.will_attempt_retry,
events.mandate as events_mandate,
events.new_customer_bank_account,
events.new_mandate,
events.organisation,
events.parent_event,
events.payment,
events.payout,
events.previous_customer_bank_account,
events.refund,
events.subscription,
payments.id as paymentID,
payments.amount ::float / 100 as amount,
payments.amount_refunded ::float / 100 as amount_refunded,
payments.created_at,
payments.charge_date,
payments.status,
man.mandate_id,
payments.description,
payments.reference,
payments.payout,
payouts.payout_id,
events_payout.payout_month,
client.client_id as customers_id,
lkp.igl_acc_id as ensekAccountId
from cte_events_payout events_payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
inner join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on payments.id = events.payment
inner join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts
on payouts.payout_id = events_payout.payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
on man.mandate_id = payments.mandate --- goCardless CLIENTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
on client.client_id = man.customerid --- ref_cdb_users ---
left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'chargeback_settled'
)


, cte_fundsReturned as (
---- Event-Refunds -- FundsReturned -----

select distinct events.id,
events.created_at as events_created_at,
events.resource_type,
events.action,
events.customer_notifications,
events.cause,
events.description,
events.origin,
events.reason_code,
events.scheme,
events.will_attempt_retry,
events.mandate as events_mandate,
events.new_customer_bank_account,
events.new_mandate,
events.organisation,
events.parent_event,
events.payment,
events.payout,
events.previous_customer_bank_account,
events.refund,
events.subscription,
refunds.id as refundID,
refunds.amount ::float / 100 as amount,
refunds.payment as paymentID,
refunds.created_at as refunds_created_at,
payments.status as payment_status,
payments.payout as payoutID,
payouts.payout_id,
events_payout.payout_month,
man.mandate_id,
client.client_id as customers_id,
lkp.igl_acc_id as ensekAccountId
from cte_events_payout events_payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_events events on events_payout.id = events.parent_event
inner join aws_fin_stage1_extracts.fin_go_cardless_api_refunds refunds on events.refund = refunds.id
left join aws_fin_stage1_extracts.fin_go_cardless_api_payments payments on refunds.payment = payments.id
left join aws_fin_stage1_extracts.fin_go_cardless_api_payouts payouts on payouts.payout_id = events_payout.payout
inner join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
on man.mandate_id = refunds.mandate --- goCardless CLIENTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
on client.client_id = man.customerid --- ref_cdb_users ---
left join public.vw_gocardless_customer_id_mapping lkp on lkp.client_id = client.client_id
where events.action = 'funds_returned'
)


, cte_summary_1 as (select payouts.payout_id,
substring (created_at, 1, 10)                                   payouts_created_date,
payouts.payout_month,
payouts.created_at,
payouts.amount ::float / 100 as payoutAmount,
payments.payments_amount  as payments_amount,
refunds.refunds_amount  as refunds_amount,
lateFailure.lateFailure_amount  as lateFailure_amount,
chargeBack.chargeBack_amount  as chargeBack_amount,
fundsReturned.fundsReturned_amount   as fundsReturned_amount
from cte_payouts payouts
left join (select payout_id, sum (amount) as payments_amount
from cte_payments
group by payout_id) payments on payouts.payout_id = payments.payout_id
left join (select payout_id, sum (amount) as refunds_amount
from cte_refundsSettled
group by payout_id) refunds on payouts.payout_id = refunds.payout_id
left join (select payout_id, sum (amount) as lateFailure_amount
from cte_lateFailure
group by payout_id) lateFailure on payouts.payout_id = lateFailure.payout_id
left join (select payout_id, sum (amount) as chargeBack_amount
from cte_chargeBack
group by payout_id) chargeBack on payouts.payout_id = chargeBack.payout_id
left join (select payout_id, sum (amount) as fundsReturned_amount
from cte_fundsReturned
group by payout_id) fundsReturned on payouts.payout_id = fundsReturned.payout_id
order by 1, 2, 3
)


, cte_output as (
--- OUTPUT -----
select stg.*,
(payoutAmount - PayoutReconcilation) as discrepancy_amount,
CASE
--- WHEN payoutAmount = PayoutReconcilation then 0 ---
WHEN (payoutAmount - PayoutReconcilation) between -0.02 and 0.02 then 0
ELSE 1
END as discrepancyFlag
from (select *,
(nvl(payments_amount, 0.0) + nvl(fundsReturned_amount, 0.0)) -
(nvl(refunds_amount, 0.0) + nvl(lateFailure_amount, 0.0) +
nvl(chargeBack_amount, 0.0)) as PayoutReconcilation
from cte_summary_1) stg
order by 1, 2, 3
)


select *
from cte_output




    WITH NO SCHEMA BINDING
;

alter table vw_fin_gc_payouts_reconciliation owner to igloo
;

