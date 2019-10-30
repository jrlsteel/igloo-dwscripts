create table ref_occupier_accounts
(
	account_id bigint null,
	cot_date timestamp null,
	current_balance double null,
	days_since_cot bigint null,
	etl_change timestamp null
)
;

create table ref_calculated_tariff_accounts
(
	account_id bigint null,
	tariff_id int null,
	start_date timestamp null,
	end_date timestamp null
)
;

create table ref_tariffs
(
	id int null,
	fuel_type varchar(1) null,
	gsp_ldz varchar(2) null,
	name varchar(20) null,
	billing_start_date timestamp null,
	signup_start_date timestamp null,
	end_date timestamp null,
	standing_charge double null,
	unit_rate double null,
	discounts varchar(50) null,
	tariff_type varchar(20) null,
	exit_fees double null
)
;

