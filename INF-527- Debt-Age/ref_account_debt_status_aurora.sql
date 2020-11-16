create table if not exists ref_account_debt_status
(
	user_id bigint,
	contract_id bigint ,
	contract_type varchar(13),
	current_account_balance double precision,
	current_direct_debit bigint ,
	bills_outstanding bigint ,
	transaction_id bigint ,
	bill_date timestamp ,
	bill_age bigint ,
	hold_days integer ,
	adjusted_bill_age bigint ,
	bill_amount integer ,
	value_paid_off bigint ,
	outstanding_value bigint ,
	payment_method varchar(4),
  etlchange timestamp
)
;
