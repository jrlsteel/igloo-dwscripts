create table ref_occupier_accounts_raw
(
	account_id bigint,
	cot_date timestamp,
	current_balance double precision,
	days_since_cot bigint,
	etl_change timestamp
)
;

alter table ref_occupier_accounts_raw owner to igloo
;

create table ref_occupier_accounts
(
	account_id bigint,
	cot_date timestamp,
	current_balance double precision,
	days_since_cot bigint,
	etl_change timestamp
)
;

alter table ref_occupier_accounts owner to igloo
;

create table ref_occupier_accounts_archive
(
	account_id bigint distkey,
	cot_date timestamp,
	current_balance double precision,
	days_since_cot bigint,
	etl_change timestamp,
	archive_date timestamp
)
diststyle key
;

alter table ref_occupier_accounts_archive owner to igloo
;

create table ref_tariffs
(
	id integer,
	fuel_type varchar(1),
	gsp_ldz varchar(2),
	name varchar(20) default 'Igloo Pioneer'::character varying,
	billing_start_date timestamp,
	signup_start_date timestamp,
	end_date timestamp,
	standing_charge double precision,
	unit_rate double precision,
	discounts varchar(50),
	tariff_type varchar(20),
	exit_fees double precision
)
;

alter table ref_tariffs owner to igloo
;

create table ref_calculated_tariff_accounts
(
	account_id bigint,
	tariff_id integer,
	start_date timestamp,
	end_date timestamp
)
;

alter table ref_calculated_tariff_accounts owner to igloo
;

create table ref_tariff_history_generated
(
	account_id bigint encode delta distkey,
	tariff_name varchar(255),
	start_date timestamp,
	end_date timestamp,
	discounts varchar(255),
	tariff_type varchar(255),
	exit_fees varchar(255)
)
diststyle key
;

alter table ref_tariff_history_generated owner to igloo
;


