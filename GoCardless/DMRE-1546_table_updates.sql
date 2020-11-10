--- UPDATE Go Cardless Tables ---
--- Mandates ---
alter table ref_fin_gocardless_mandates
    drop column etlchange;
alter table ref_fin_gocardless_mandates
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_mandates
    add column etlchange timestamp;
-- the "where true" below is to suppress a warning of updating the whole table, which is the intended behaviour here
update ref_fin_gocardless_mandates set last_updated_igloo = created_at where true;

--- Mandates Audit ---
alter table ref_fin_gocardless_mandates_audit
    drop column etlchange;
alter table ref_fin_gocardless_mandates_audit
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_mandates_audit
    add column etlchange timestamp;
-- the "where true" below is to suppress a warning of updating the whole table, which is the intended behaviour here
update ref_fin_gocardless_mandates_audit set last_updated_igloo = created_at where true;

--- Payments ---
alter table ref_fin_gocardless_payments
    drop column etlchange;
alter table ref_fin_gocardless_payments
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_payments
    add column etlchange timestamp;
-- the "where true" below is to suppress a warning of updating the whole table, which is the intended behaviour here
update ref_fin_gocardless_payments set last_updated_igloo = created_at where true;

--- Refunds ---
alter table ref_fin_gocardless_refunds
    drop column etlchange;
alter table ref_fin_gocardless_refunds
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_refunds
    add column etlchange timestamp;
-- the "where true" below is to suppress a warning of updating the whole table, which is the intended behaviour here
update ref_fin_gocardless_refunds set last_updated_igloo = created_at where true;

--- Refunds Audit ---
alter table ref_fin_gocardless_refunds_audit
    drop column etlchange;
alter table ref_fin_gocardless_refunds_audit
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_refunds_audit
    add column etlchange timestamp;
-- the "where true" below is to suppress a warning of updating the whole table, which is the intended behaviour here
update ref_fin_gocardless_refunds_audit set last_updated_igloo = created_at where true;

--- Subscriptions ---
alter table ref_fin_gocardless_subscriptions
    drop column etlchange;
alter table ref_fin_gocardless_subscriptions
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_subscriptions
    add column etlchange timestamp;
-- the "where true" below is to suppress a warning of updating the whole table, which is the intended behaviour here
update ref_fin_gocardless_subscriptions set last_updated_igloo = created_at where true;

--- Subscriptions Audit ---
alter table ref_fin_gocardless_subscriptions_audit
    drop column etlchange;
alter table ref_fin_gocardless_subscriptions_audit
    add column last_updated_igloo timestamp;
alter table ref_fin_gocardless_subscriptions_audit
    add column etlchange timestamp;
-- the "where true" below is to suppress a warning of updating the whole table, which is the intended behaviour here
update ref_fin_gocardless_subscriptions_audit set last_updated_igloo = created_at where true;