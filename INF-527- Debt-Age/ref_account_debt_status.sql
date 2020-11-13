create table ref_account_debt_status
(
	user_id integer encode az64,
	contract_id integer encode az64,
	contract_type varchar(256),
	current_account_balance integer encode az64,
	current_direct_debit integer encode az64,
	bills_outstanding integer encode az64,
	transaction_id integer encode az64,
	bill_date timestamp encode az64,
	bill_age integer encode az64
)
;




alter table ref_account_debt_status owner to igloo
;

