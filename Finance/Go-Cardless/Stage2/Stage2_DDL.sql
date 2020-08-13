
/*
DDL SCRIPTS for STAGE2 Go-Cardless
*/

---------------------------- TABLES ----------------------------------



create table ref_fin_gocardless_mandates
(
	mandate_id Varchar(50) distkey,
	customerid Varchar(50),
	new_mandate_id Varchar(50),
	created_at timestamp,
	next_possible_charge_date timestamp,
	payments_require_approval Varchar(50),
	reference Varchar(250),
	scheme Varchar(50),
	status Varchar(50),
	creditor Varchar(150),
	customer_bank_account Varchar(150),
	ensekid Varchar(50),
	ensekstatementid Varchar(50) ,
	etlchange timestamp
)
diststyle key
sortkey(mandate_id)
;

alter table ref_fin_gocardless_mandates owner to igloo
;






create table ref_fin_gocardless_refunds
(
	ensekid Varchar(50),
	amount bigint,
	created_at timestamp,
	currency Varchar(50),
	id Varchar(50) distkey,
	mandate Varchar(50),
	metadata Varchar(50),
	payment Varchar(50),
	reference Varchar(250),
	status Varchar(50),
	etlchange timestamp
)
diststyle key
sortkey(id)
;

alter table ref_fin_gocardless_refunds owner to igloo
;







create table ref_fin_gocardless_subscriptions
(
	id Varchar(50) distkey,
	created_at timestamp,
	amount bigint,
	currency Varchar(50),
	status Varchar(50),
	name Varchar(50),
	start_date timestamp,
	end_date timestamp,
	interval Varchar(50),
	interval_unit Varchar(50),
	day_of_month bigint,
	month Varchar(50),
	count_no Varchar(50),
	payment_reference Varchar(250),
	app_fee Varchar(50),
	retry_if_possible Varchar(50),
	mandate Varchar(50),
	charge_date timestamp,
	amount_subscription bigint,
	etlchange timestamp
)
diststyle key
sortkey(id)
;

alter table ref_fin_gocardless_subscriptions owner to igloo
;








create table ref_fin_gocardless_payments
(
	id Varchar(50) distkey ,
	amount bigint,
	amount_refunded bigint,
	charge_date timestamp,
	created_at timestamp,
	currency Varchar(50),
	description Varchar(250),
	reference Varchar(250),
	status Varchar(50),
	payout Varchar(50),
	mandate Varchar(50),
	subscription Varchar(50),
	ensekid Varchar(50),
	statementid Varchar(50) ,
	etlchange timestamp
)
diststyle key
sortkey(id)
;

alter table ref_fin_gocardless_payments owner to igloo
;








-------------------------- AUDIT -----------------------------------------



create table ref_fin_gocardless_mandates_audit
(
	mandate_id Varchar(50) distkey,
	customerid Varchar(50),
	new_mandate_id Varchar(50),
	created_at timestamp,
	next_possible_charge_date timestamp,
	payments_require_approval Varchar(50),
	reference Varchar(250),
	scheme Varchar(50),
	status Varchar(50),
	creditor Varchar(150),
	customer_bank_account Varchar(150),
	ensekid Varchar(50),
	ensekstatementid Varchar(50) ,
	etlchangetype varchar(1),
	etlchange timestamp
)
diststyle key
sortkey(mandate_id)
;

alter table ref_fin_gocardless_mandates_audit owner to igloo
;






create table ref_fin_gocardless_refunds_audit
(
	ensekid Varchar(50),
	amount bigint,
	created_at timestamp,
	currency Varchar(50),
	id Varchar(50) distkey,
	mandate Varchar(50),
	metadata Varchar(50),
	payment Varchar(50),
	reference Varchar(250),
	status Varchar(50),
	etlchangetype varchar(1),
	etlchange timestamp
)
diststyle key
sortkey(id)
;

alter table ref_fin_gocardless_refunds_audit owner to igloo
;








create table ref_fin_gocardless_subscriptions_audit
(
	id Varchar(50) distkey,
	created_at timestamp,
	amount bigint,
	currency Varchar(50),
	status Varchar(50),
	name Varchar(50),
	start_date timestamp,
	end_date timestamp,
	interval Varchar(50),
	interval_unit Varchar(50),
	day_of_month bigint,
	month Varchar(50),
	count_no Varchar(50),
	payment_reference Varchar(250),
	app_fee Varchar(50),
	retry_if_possible Varchar(50),
	mandate Varchar(50),
	charge_date timestamp,
	amount_subscription bigint,
	etlchangetype varchar(1),
	etlchange timestamp
)
diststyle key
sortkey(id)
;

alter table ref_fin_gocardless_subscriptions_audit owner to igloo
;







create table ref_fin_gocardless_payments_audit
(
	id Varchar(50) distkey ,
	amount bigint,
	amount_refunded bigint,
	charge_date timestamp,
	created_at timestamp,
	currency Varchar(50),
	description Varchar(250),
	reference Varchar(250),
	status Varchar(50),
	payout Varchar(50),
	mandate Varchar(50),
	subscription Varchar(50),
	ensekid Varchar(50),
	statementid Varchar(50) ,
	etlchangetype varchar(1),
	etlchange timestamp
)
diststyle key
sortkey(id)
;

alter table ref_fin_gocardless_payments_audit owner to igloo
;





























