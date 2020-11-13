create table ref_account_debt_status
(
	user_id int,
	contract_id int,
	contract_type varchar(256),
	current_account_balance int,
	current_direct_debit int,
	bills_outstanding int,
	transaction_id int,
	bill_date timestamp,
	bill_age int
)
;
