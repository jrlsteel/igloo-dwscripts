/*
 Test 1
 Expectation: returns null
 */
select payout_id, count(*)
from aws_fin_stage1_extracts.fin_go_cardless_api_payouts
group by payout_id
having count(*) > 1;

/*
 Test 2.1
 Expectation: 0
 */
select count(*)
from ref_fin_gocardless_mandates
where last_updated_igloo is null;

/*
 Test 2.2
 Expectation: 0
 */
select count(*)
from ref_fin_gocardless_payments
where last_updated_igloo is null;

/*
 Test 2.3
 Expectation: 0
 */
select count(*)
from ref_fin_gocardless_subscriptions
where last_updated_igloo is null;

/*
 Test 2.4
 Expectation: 0
 */
select count(*)
from ref_fin_gocardless_refunds
where last_updated_igloo is null;

/*
 Test 3.1
 Expectation: null
 */
select *
from vw_gc_updates_mandates
where mandate_id in (select mandate_id from ref_fin_gocardless_mandates);

/*
 Test 3.2
 Expectation: null
 */
select *
from vw_gc_updates_subscriptions
where subscription_id in (select id from ref_fin_gocardless_subscriptions);

/*
 Test 3.3
 Expectation: null
 */
select *
from vw_gc_updates_payments
where payment_id in (select id from ref_fin_gocardless_payments);

/*
 Test 3.4
 Expectation: null
 */
select *
from vw_gc_updates_refunds
where refund_id in (select id from ref_fin_gocardless_refunds);