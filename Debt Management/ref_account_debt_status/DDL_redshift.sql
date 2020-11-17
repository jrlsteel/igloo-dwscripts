create table ref_account_debt_status
(
	user_id bigint encode az64 distkey,
	contract_id bigint encode az64,
	contract_type varchar(13),
	current_account_balance double precision,
	current_direct_debit bigint encode az64,
	bills_outstanding bigint encode az64,
	transaction_id bigint encode az64,
	bill_date timestamp encode az64,
	bill_age bigint encode az64,
	hold_days integer encode az64,
	adjusted_bill_age bigint encode az64,
	bill_amount integer encode az64,
	value_paid_off bigint encode az64,
	outstanding_value bigint encode az64,
	payment_method varchar(4),
  etlchange timestamp
)
diststyle key
;

alter table ref_account_debt_status owner to igloo
;