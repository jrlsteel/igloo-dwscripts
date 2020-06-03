create or replace view vw_fin_gc_to_sl_reconciliation as

with cte_payouts as (
select *
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


, cte_ensek as (
select *,
CASE WHEN substring (createddate, 9, 2)::int >= 26 AND substring (createddate, 7, 2)::int = 2
THEN substring (dateadd(month, 1, createddate::timestamp), 1, 7)
WHEN substring (createddate, 9, 2)::int >= 26 AND substring (createddate, 7, 2)::int != 2
THEN substring (dateadd(month, 1, createddate::timestamp), 1, 7)
ELSE substring (createddate, 1, 7)
END as payout_month,
substring (createddate, 1, 7) as Event_Month
from aws_fin_stage1_extracts.fin_sales_ledger_all_time
where nominal = '7603'
and substring (createddate, 1, 10) between '2017-01-01' and to_char(sysdate, 'YYYY-MM-DD')
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
lkp.igl_acc_id as ensekAccountId,

CASE WHEN substring (payments.charge_date, 9, 2)::int >= 25 AND substring (payments.charge_date, 7, 2)::int = 2
THEN substring (dateadd(month, 1, payments.charge_date::timestamp), 1, 7)
WHEN substring (payments.charge_date, 9, 2)::int >= 25 AND substring (payments.charge_date, 7, 2)::int != 2
THEN substring (dateadd(month, 1, payments.charge_date::timestamp), 1, 7)
ELSE substring (payments.charge_date, 1, 7)
END as payout_month_ml,

substring (payments.charge_date, 1, 7) as Event_Month

from cte_events_payout as events_payout
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
lkp.igl_acc_id as ensekAccountId,

CASE WHEN substring (refunds.created_at, 9, 2)::int >= 26 AND substring (refunds.created_at, 7, 2)::int = 2
THEN substring (dateadd(month, 1, refunds.created_at::timestamp), 1, 7)
WHEN substring (refunds.created_at, 9, 2)::int >= 26 AND substring (refunds.created_at, 7, 2)::int != 2
THEN substring (dateadd(month, 1, refunds.created_at::timestamp), 1, 7)
ELSE substring (refunds.created_at, 1, 7)
END as payout_month_ml,

substring (refunds.created_at, 1, 7) as Event_Month

from cte_events_payout as events_payout
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
from cte_events_payout as events_payout
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
from cte_events_payout as events_payout
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
from cte_events_payout as events_payout
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


, cte_payments_summ as (
select
ensekAccountId,
created_at,
charge_date as Payment_ChargeDate,
Event_Month as payout_month,
amount,
row_number() over (partition by ensekAccountId,
Event_Month, amount order by gc.created_at asc) as Countif
from cte_payments gc
where (ensekAccountId is not null)
)

, cte_refunds_summ as (
select
ensekAccountId,
events_created_at,
substring (refunds_created_at, 1, 10) as refunds_created_at,
Event_Month as payout_month,
amount,
row_number() over (partition by ensekAccountId,
Event_Month, amount order by refunds_created_at asc) as Countif
from cte_refundsSettled
where (ensekAccountId is not null)
)

, cte_ensek_summ as (
select
AccountId,
CreatedDate,
payout_month,
round((((TransAmount + (0.005 * sign(TransAmount))) * 100)::int) / 100.0, 2) as TransAmount,
Countif
from
(
select
AccountId,
CreatedDate,
Event_Month as payout_month,
TransAmount,
row_number() over (partition by AccountId, Event_Month, TransAmount
order by accounttransactionid asc) as Countif
from cte_ensek
where (AccountId is not null)
) ezk
)


, cte_chargeBack_summ as (
select
ensekAccountId,
substring (created_at, 1, 10) as created_at,
payout_month,
amount,
row_number() over (partition by ensekAccountId,
payout_month, amount order by gc.created_at asc) as Countif
from cte_chargeBack gc
where (ensekAccountId is not null)
)


, cte_lateFailure_summ as (
select
ensekAccountId,
substring (created_at, 1, 10) as created_at,
payout_month,
amount,
row_number() over (partition by ensekAccountId,
payout_month, amount order by gc.created_at asc) as Countif
from cte_lateFailure gc
where (ensekAccountId is not null)
)


, cte_fundsReturned_summ as (
select
ensekAccountId,
substring (refunds_created_at, 1, 10) as created_at,
payout_month,
amount,
row_number() over (partition by ensekAccountId,
payout_month, amount order by gc.refunds_created_at asc) as Countif
from cte_fundsReturned gc
where (ensekAccountId is not null)
)


, cte_GoCardless_payments_tab as (
select
gc.ensekAccountId,
gc.created_at,
gc.Payment_ChargeDate,
gc.payout_month,
gc.amount,
ensek.TransAmount,
gc.Countif,
nvl(ensek.Countif, 0) as PaymentInEnsekFlag
from cte_payments_summ gc
left join cte_ensek_summ ensek
on gc.ensekAccountId = ensek.AccountId and
gc.amount = ensek.TransAmount and
(
gc.payout_month = ensek.payout_month or
(
substring (dateadd(month, 1, gc.Payment_ChargeDate::timestamp), 1, 7) = ensek.payout_month
and datediff(days, gc.Payment_ChargeDate::timestamp, ensek.createddate::timestamp) < 25
)
) and
gc.Countif = ensek.Countif and
substring (gc.created_at, 1, 10)::timestamp <= substring (ensek.CreatedDate::timestamp, 1, 10)

)

, cte_GoCardless_refunds_tab as (
select
gc.ensekAccountId,
gc.events_created_at as created_at,
gc.refunds_created_at,
gc.payout_month,
gc.amount,
ensek.TransAmount,
gc.Countif,
nvl(ensek.Countif, 0) as RefundInEnsekFlag
from cte_refunds_summ gc
left join cte_ensek_summ ensek
on gc.ensekAccountId = ensek.AccountId and
(gc.amount * -1.0) = ensek.TransAmount and
(
gc.payout_month = ensek.payout_month or
(
substring (dateadd(month, 1, gc.refunds_created_at::timestamp), 1, 7) = ensek.payout_month
and datediff(days, gc.refunds_created_at::timestamp, ensek.createddate::timestamp) < 25
)
) and
gc.Countif = ensek.Countif and
substring (gc.refunds_created_at, 1, 10)::timestamp <= substring (ensek.CreatedDate::timestamp, 1, 10) and
ensek.TransAmount < 0 ---- REFUNDS ONLY ----
)


, cte_ensek_payments_tab as (
select
ensek.AccountId,
ensek.CreatedDate,
ensek.payout_month,
ensek.TransAmount,
ensek.Countif,
CASE WHEN ensek.TransAmount > 0 then ensek.TransAmount
ELSE NULL
END as ensekPaymentsAmount,
CASE WHEN ensek.TransAmount < 0 then ensek.TransAmount
ELSE NULL
END as ensekRefundsAmount,
ref.amount as refundAmount,
paym.amount as paymentAmount,
paym.Countif as C_Pay,
ref.Countif as C_Ref,
CASE WHEN coalesce (paym.Countif, ref.Countif, 0) = 0
THEN 0
ELSE 1
END as inGCFlag
from cte_ensek_summ ensek
left join cte_refunds_summ ref
on ref.ensekAccountId = ensek.AccountId and
(ref.amount * -1.0) = ensek.TransAmount and
---- ref.payout_month = ensek.payout_month and
(
ref.payout_month = ensek.payout_month or
(
substring (dateadd(month, 1, ref.refunds_created_at ::timestamp), 1, 7) = ensek.payout_month
and datediff(days, ref.refunds_created_at::timestamp, ensek.createddate::timestamp) < 25
)
) and
ref.Countif = ensek.Countif and
substring (ref.refunds_created_at, 1, 10)::timestamp <= substring (ensek.CreatedDate::timestamp, 1, 10) and
ensek.TransAmount < 0 ---- REFUNDS ONLY ----
left join cte_payments_summ paym
on paym.ensekAccountId = ensek.AccountId and
paym.amount = ensek.TransAmount and
-----paym.payout_month = ensek.payout_month and
(
paym.payout_month = ensek.payout_month or
(
substring (dateadd(month, 1, paym.Payment_ChargeDate ::timestamp), 1, 7) = ensek.payout_month
and datediff(days, paym.Payment_ChargeDate::timestamp, ensek.createddate::timestamp) < 25
)
) and
paym.Countif = ensek.Countif and
substring (paym.created_at, 1, 10)::timestamp <= substring (ensek.CreatedDate::timestamp, 1, 10)
)


, cte_GoCardless_chargeBack_tab as (
select
gc.ensekAccountId,
gc.created_at,
gc.payout_month,
gc.amount,
ensek.TransAmount,
nvl(ensek.Countif, 0) as PaymentInEnsekFlag
from cte_chargeBack gc
left join cte_ensek_summ ensek
on gc.ensekAccountId = ensek.AccountId and
gc.amount = ensek.TransAmount and
gc.payout_month = ensek.payout_month

)


, cte_GoCardless_lateFailure_tab as (
select
gc.ensekAccountId,
gc.created_at,
gc.payout_month = ensek.payout_month,
gc.amount,
ensek.TransAmount,
nvl(ensek.Countif, 0) as PaymentInEnsekFlag
from cte_lateFailure gc
left join cte_ensek_summ ensek
on gc.ensekAccountId = ensek.AccountId and
gc.amount = ensek.TransAmount and
gc.payout_month = ensek.payout_month

)


, cte_GoCardless_fundsReturned_tab as (
select
gc.ensekAccountId,
gc.refunds_created_at as created_at,
gc.payout_month,
gc.amount,
ensek.TransAmount,
nvl(ensek.Countif, 0) as RefundInEnsekFlag
from cte_fundsReturned gc
left join cte_ensek_summ ensek
on gc.ensekAccountId = ensek.AccountId and
(gc.amount * -1.0) = ensek.TransAmount and
gc.payout_month = ensek.payout_month and
ensek.TransAmount < 0 ---- REFUNDS ONLY ----
)


, cte_report_summary as (

SELECT
distinct
nvl(GC.ensekAccountId, ensek.accountid) as ensekAccountId,
nvl(GC.payout_month, ensek.payout_month) as payout_month,
GC.GCCreatedAtDate,
GC.GCAmount,
GC.GCPaymentAmount,
GC.GCRefundAmount,
GC.PaymentInEnsekFlag as Payment_In_Ensek_Flag,
GC.RefundInEnsekFlag as Refund_In_Ensek_Flag,
ensek.createddate as EnsekCreatedAtDate,
ensek.transamount as EnsekAmount,
ensek.ensekPaymentsAmount,
ensek.ensekRefundsAmount,
ensek.inGCFlag as Ensek_In_GC_Flag,
ensek.C_Ref,
ensek.C_Pay
FROM
(
Select
pay.ensekAccountId,
pay.created_at as GCCreatedAtDate,
pay.payout_month,
pay.amount as GCAmount,
pay.amount as GCPaymentAmount,
null as GCRefundAmount,
pay.PaymentInEnsekFlag,
0 as RefundInEnsekFlag
from cte_GoCardless_payments_tab pay

UNION

select
ref.ensekAccountId,
ref.created_at as GCCreatedAtDate,
ref.payout_month,
(ref.amount * -1.0) as GCAmount,
null as GCPaymentAmount,
(ref.amount * -1.0) as GCRefundAmount,
0 as PaymentInEnsekFlag,
ref.RefundInEnsekFlag
from cte_GoCardless_refunds_tab ref
) GC
full outer join cte_ensek_payments_tab ensek
on ensek.accountid = GC.ensekAccountId
and ensek.transamount = GC.GCAmount
and ensek.payout_month = GC.payout_month
order by 1, 2

)


, cte_report_summary_GC as (

SELECT
distinct
GC.ensekAccountId as ensekAccountId,
GC.payout_month as payout_month,
GC.GCCreatedAtDate,
GC.GCPayment_ChargeDate,
GC.GCRefund_Date,
GC.GCAmount,
GC.GCPaymentAmount,
GC.GCRefundAmount,
GC.PaymentInEnsekFlag as Payment_In_Ensek_Flag,
GC.RefundInEnsekFlag as Refund_In_Ensek_Flag,
CASE WHEN GC.PaymentInEnsekFlag >= 1 or GC.RefundInEnsekFlag >= 1 THEN 0
ELSE 1
END as In_GC_not_in_Ensek_Flag,
ensek.createddate as EnsekCreatedAtDate,
ensek.transamount as EnsekAmount,
ensek.ensekPaymentsAmount,
ensek.ensekRefundsAmount,
ensek.inGCFlag as Ensek_In_GC_Flag,
ensek.C_Ref,
ensek.C_Pay
FROM
(
Select
pay.ensekAccountId,
pay.created_at as GCCreatedAtDate,
pay.Payment_ChargeDate as GCPayment_ChargeDate,
null as GCRefund_Date,
pay.payout_month,
pay.amount as GCAmount,
pay.amount as GCPaymentAmount,
null as GCRefundAmount,
pay.PaymentInEnsekFlag,
0 as RefundInEnsekFlag,
Countif
from cte_GoCardless_payments_tab pay

UNION

select
ref.ensekAccountId,
ref.created_at as GCCreatedAtDate,
null as GCPayment_ChargeDate,
ref.refunds_created_at as GCRefund_Date,
ref.payout_month,
(ref.amount * -1.0) as GCAmount,
null as GCPaymentAmount,
(ref.amount * -1.0) as GCRefundAmount,
0 as PaymentInEnsekFlag,
ref.RefundInEnsekFlag,
Countif
from cte_GoCardless_refunds_tab ref
) GC
left join cte_ensek_payments_tab ensek
on ensek.accountid = GC.ensekAccountId
and ensek.transamount = GC.GCAmount
---and ensek.payout_month = GC.payout_month
and (
ensek.payout_month = GC.payout_month or
(
substring (dateadd(month, 1, nvl(GC.GCPayment_ChargeDate, GC.GCRefund_Date) ::timestamp), 1, 7) = ensek.payout_month
and datediff(days, nvl(GC.GCPayment_ChargeDate, GC.GCRefund_Date)::timestamp, ensek.createddate::timestamp) < 25
)
)
and ensek.Countif = GC.Countif
order by 1, 2

)


, cte_report_summary_ensek as (

SELECT
distinct
ensek.accountid as ensekAccountId,
ensek.payout_month as payout_month,
ensek.createddate as EnsekCreatedAtDate,
ensek.transamount as EnsekAmount,
ensek.ensekPaymentsAmount,
ensek.ensekRefundsAmount,
ensek.inGCFlag as Ensek_In_GC_Flag,
CASE WHEN ensek.inGCFlag >= 1 THEN 0
ELSE 1
END as In_Ensek_not_in_GC_Flag,
ensek.C_Ref,
ensek.C_Pay,
GC.GCCreatedAtDate,
GC.GCPayment_ChargeDate,
GC.GCRefund_Date,
GC.GCAmount,
GC.GCPaymentAmount,
GC.GCRefundAmount,
GC.PaymentInEnsekFlag as Payment_In_Ensek_Flag,
GC.RefundInEnsekFlag as Refund_In_Ensek_Flag
FROM
(
Select
pay.ensekAccountId,
pay.created_at as GCCreatedAtDate,
pay.Payment_ChargeDate as GCPayment_ChargeDate,
null as GCRefund_Date,
pay.payout_month,
pay.amount as GCAmount,
pay.amount as GCPaymentAmount,
null as GCRefundAmount,
pay.PaymentInEnsekFlag,
0 as RefundInEnsekFlag,
Countif
from cte_GoCardless_payments_tab pay

UNION

select
ref.ensekAccountId,
ref.created_at as GCCreatedAtDate,
null as GCPayment_ChargeDate,
ref.refunds_created_at as GCRefund_Date,
ref.payout_month,
(ref.amount * -1.0) as GCAmount,
null as GCPaymentAmount,
(ref.amount * -1.0) as GCRefundAmount,
0 as PaymentInEnsekFlag,
ref.RefundInEnsekFlag,
Countif
from cte_GoCardless_refunds_tab ref
) GC
right join cte_ensek_payments_tab ensek
on ensek.accountid = GC.ensekAccountId
and ensek.transamount = GC.GCAmount
--- and ensek.payout_month = GC.payout_month
and (
ensek.payout_month = GC.payout_month or
(
substring (dateadd(month, 1, nvl(GC.GCPayment_ChargeDate, GC.GCRefund_Date) ::timestamp), 1, 7) = ensek.payout_month
and datediff(days, nvl(GC.GCPayment_ChargeDate, GC.GCRefund_Date)::timestamp, ensek.createddate::timestamp) < 25
)
)
and ensek.Countif = GC.Countif
order by 1, 2

)


, cte_summary_check as (select sum (CASE
WHEN gcamount > 0 THEN gcamount
ELSE 0
END) as GCTotal_Payment,
sum (CASE
WHEN gcamount < 0 THEN gcamount
ELSE 0
END) as GCTotal_Refund,
sum (CASE
WHEN EnsekAmount > 0 THEN EnsekAmount
ELSE 0
END) as Ensek_Payment,
sum (CASE
WHEN EnsekAmount < 0 THEN EnsekAmount
ELSE 0
END) as Ensek_Refund
from cte_report_summary
)


, cte_report_summary_GC01 as (
select
ensekAccountId,
payout_month,
GCCreatedAtDate,
GCPayment_ChargeDate,
GCRefund_Date,
GCAmount,
GCPaymentAmount,
GCRefundAmount,
CASE WHEN EnsekCreatedAtDate is NULL THEN 1
ELSE 0
END as GC_Only_Flag,
Payment_In_Ensek_Flag,
Refund_In_Ensek_Flag,
In_GC_not_in_Ensek_Flag,
EnsekCreatedAtDate,
EnsekAmount,
ensekPaymentsAmount,
ensekRefundsAmount,
CASE WHEN GCCreatedAtDate is NULL THEN 1
ELSE 0
END as Ensek_Only_Flag,
Ensek_In_GC_Flag,
C_Ref,
C_Pay,
CASE WHEN GCRefund_Date is not null
THEN
((DATEDIFF(day, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp, EnsekCreatedAtDate::timestamp) + 1)
-(DATEDIFF(week, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp, EnsekCreatedAtDate::timestamp) * 2)
-(CASE WHEN date_part(dow, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp) = 0 THEN 1 ELSE 0 END)
-(CASE WHEN date_part(dow, EnsekCreatedAtDate::timestamp) = 6 THEN 1 ELSE 0 END))
- 1
WHEN GCPayment_ChargeDate is not null
THEN
((DATEDIFF(day, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp, EnsekCreatedAtDate::timestamp) + 1)
-(DATEDIFF(week, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp, EnsekCreatedAtDate::timestamp) * 2)
-(CASE WHEN date_part(dow, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp) = 0 THEN 1 ELSE 0 END)
-(CASE WHEN date_part(dow, EnsekCreatedAtDate::timestamp) = 6 THEN 1 ELSE 0 END))
-1
ELSE NULL
END as GC_SL_Date_Difference
from cte_report_summary_GC
)


, cte_report_summary_SL01 as (
select
ensekAccountId,
payout_month,
EnsekCreatedAtDate,
EnsekAmount,
ensekPaymentsAmount,
ensekRefundsAmount,
CASE WHEN GCCreatedAtDate is NULL THEN 1
ELSE 0
END as Ensek_Only_Flag,
Ensek_In_GC_Flag,
In_Ensek_not_in_GC_Flag,
C_Ref,
C_Pay,
GCCreatedAtDate,
GCPayment_ChargeDate,
GCRefund_Date,
GCAmount,
GCPaymentAmount,
GCRefundAmount,
CASE WHEN EnsekCreatedAtDate is NULL THEN 1
ELSE 0
END as GC_Only_Flag,
Payment_In_Ensek_Flag,
Refund_In_Ensek_Flag,
CASE WHEN GCRefund_Date is not null
THEN
((DATEDIFF(day, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp, EnsekCreatedAtDate::timestamp) + 1)
-(DATEDIFF(week, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp, EnsekCreatedAtDate::timestamp) * 2)
-(CASE WHEN date_part(dow, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp) = 0 THEN 1 ELSE 0 END)
-(CASE WHEN date_part(dow, EnsekCreatedAtDate::timestamp) = 6 THEN 1 ELSE 0 END))
- 1
WHEN GCPayment_ChargeDate is not null
THEN
((DATEDIFF(day, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp, EnsekCreatedAtDate::timestamp) + 1)
-(DATEDIFF(week, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp, EnsekCreatedAtDate::timestamp) * 2)
-(CASE WHEN date_part(dow, substring (nvl(GCPayment_ChargeDate, GCRefund_Date), 1, 10)::timestamp) = 0 THEN 1 ELSE 0 END)
-(CASE WHEN date_part(dow, EnsekCreatedAtDate::timestamp) = 6 THEN 1 ELSE 0 END))
-1
ELSE NULL
END as GC_SL_Date_Difference

from cte_report_summary_ensek
)


, cte_report_summary_GC02 as (
select *,
CASE WHEN EnsekCreatedAtDate is null THEN 1
WHEN GC_SL_Date_Difference > 2 THEN 1
WHEN GC_SL_Date_Difference <= 2 THEN 0
ELSE 1
END as discrepancy_flag_day_1,
CASE WHEN gcpaymentamount is not null or ensekpaymentsamount is not null THEN 'Payments'
WHEN GCRefundAmount is not null or ensekRefundsAmount is not null THEN 'Refunds'
ELSE 'NA'
END as Event_Type
from cte_report_summary_GC01
)


, cte_report_summary_SL02 as (
select *,
CASE WHEN EnsekCreatedAtDate is null THEN 1
WHEN GC_SL_Date_Difference > 2 THEN 1
WHEN GC_SL_Date_Difference <= 2 THEN 0
ELSE 1
END as discrepancy_flag_day_1,
CASE WHEN gcpaymentamount is not null or ensekpaymentsamount is not null THEN 'Payments'
WHEN GCRefundAmount is not null or ensekRefundsAmount is not null THEN 'Refunds'
ELSE 'NA'
END as Event_Type
from cte_report_summary_SL01
)


select *
from cte_report_summary_GC02
order by 1, 2, 3


    WITH NO SCHEMA BINDING
;

alter table vw_fin_gc_to_sl_reconciliation owner to igloo
;

