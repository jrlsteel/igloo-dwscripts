-- mandates
create or replace view vw_gc_updates_mandates as
select distinct mandate_events.mandate as mandate_id
from aws_fin_stage1_extracts.fin_go_cardless_api_events mandate_events
         left join public.ref_fin_gocardless_mandates existing_mandates
                   on mandate_events.mandate = existing_mandates.mandate_id
where mandate_events.resource_type = 'mandates'
  and (mandate_events.created_at::timestamp > nvl(existing_mandates.created_at, '2000-01-01')::timestamp)
  and mandate_events.created_at between '2020-10-01' and '2020-11-01'
  and len(mandate_events.mandate) > 0
order by mandate_events.mandate
with no schema binding
;

select (count(*)/20.0)/60.0 from vw_gc_updates_mandates

-- subscriptions
create or replace view vw_gc_updates_subscriptions as
select distinct subscription_events.subscription as subscription_id
from aws_fin_stage1_extracts.fin_go_cardless_api_events subscription_events
         left join public.ref_fin_gocardless_subscriptions existing_subscriptions
                   on subscription_events.subscription = existing_subscriptions.id
where subscription_events.resource_type = 'subscriptions'
  and (subscription_events.created_at::timestamp > nvl(existing_subscriptions.created_at, '2000-01-01')::timestamp)
  and subscription_events.created_at between '2020-10-01' and '2020-11-01'
  and len(subscription_events.subscription) > 0
order by subscription_events.subscription
with no schema binding
;

-- payments
create or replace view vw_gc_updates_payments as
select distinct payment_events.payment as payment_id
from aws_fin_stage1_extracts.fin_go_cardless_api_events payment_events
         left join public.ref_fin_gocardless_payments existing_payments
                   on payment_events.payment = existing_payments.id
where payment_events.resource_type = 'payments'
  and (payment_events.created_at::timestamp > nvl(existing_payments.created_at, '2000-01-01')::timestamp)
  and payment_events.created_at between '2020-10-01' and '2020-11-01'
  and len(payment_events.payment) > 0
order by payment_events.payment
with no schema binding
;

-- refunds
create or replace view vw_gc_updates_refunds as
select distinct refund_events.refund as refund_id
from aws_fin_stage1_extracts.fin_go_cardless_api_events refund_events
         left join public.ref_fin_gocardless_refunds existing_refunds
                   on refund_events.refund = existing_refunds.id
where refund_events.resource_type = 'refunds'
  and (refund_events.created_at::timestamp > nvl(existing_refunds.created_at, '2000-01-01')::timestamp)
  and refund_events.created_at between '2020-10-01' and '2020-11-01'
  and len(refund_events.refund) > 0
order by refund_events.refund
with no schema binding
;
