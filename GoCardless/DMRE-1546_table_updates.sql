--- UPDATE Go Cardless Tables ---
--- Mandates ---
alter table ref_fin_gocardless_mandates
    drop column etlchange;
alter table ref_fin_gocardless_mandates
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_mandates
    add column etlchange timestamp;

--- Mandates Audit ---
alter table ref_fin_gocardless_mandates_audit
    drop column etlchange;
alter table ref_fin_gocardless_mandates_audit
    drop column etlchangetype;
alter table ref_fin_gocardless_mandates_audit
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_mandates_audit
    add column etlchangetype varchar(1);
alter table ref_fin_gocardless_mandates_audit
    add column etlchange timestamp;

--- Payments ---
alter table ref_fin_gocardless_payments
    drop column etlchange;
alter table ref_fin_gocardless_payments
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_payments
    add column etlchange timestamp;

--- Refunds ---
alter table ref_fin_gocardless_refunds
    drop column etlchange;
alter table ref_fin_gocardless_refunds
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_refunds
    add column etlchange timestamp;

--- Refunds Audit ---
alter table ref_fin_gocardless_refunds_audit
    drop column etlchange;
alter table ref_fin_gocardless_refunds_audit
    drop column etlchangetype;
alter table ref_fin_gocardless_refunds_audit
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_refunds_audit
    add column etlchangetype varchar(1);
alter table ref_fin_gocardless_refunds_audit
    add column etlchange timestamp;

--- Subscriptions ---
alter table ref_fin_gocardless_subscriptions
    drop column etlchange;
alter table ref_fin_gocardless_subscriptions
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_subscriptions
    add column etlchange timestamp;

--- Subscriptions Audit ---
alter table ref_fin_gocardless_subscriptions_audit
    drop column etlchange;
alter table ref_fin_gocardless_subscriptions_audit
    drop column etlchangetype;
alter table ref_fin_gocardless_subscriptions_audit
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_subscriptions_audit
    add column etlchangetype varchar(1);
alter table ref_fin_gocardless_subscriptions_audit
    add column etlchange timestamp;
