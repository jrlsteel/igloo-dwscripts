
select
	mandate_id
	customerid ,
	new_mandate_id ,
	created_at::timestamp,
	next_possible_charge_date::timestamp,
	payments_require_approval ,
	reference ,
	scheme ,
	status ,
	creditor ,
	customer_bank_account ,
	ensekid ,
	ensekstatementid,
    getdate()   as etlchange
from
aws_fin_stage1_extracts.fin_stage2_gocardlessmandates ;






select
	ensekid  ,
	amount::bigint,
	created_at::timestamp ,
	currency  ,
	id  ,
	mandate  ,
	metadata  ,
	payment  ,
	reference  ,
	status,
    getdate()   as etlchange
from
aws_fin_stage1_extracts.fin_stage2_gocardlessrefunds
;





SELECT
	id  ,
	created_at::timestamp,
	amount::bigint,
	currency  ,
	status  ,
	name  ,
	start_date::timestamp,
	end_date::timestamp,
	interval::bigint ,
	interval_unit ,
	day_of_month,
	month::int,
	count_no::bigint,
	payment_reference ,
	app_fee ,
	retry_if_possible ,
	mandate ,
	charge_date::timestamp ,
	amount_subscription::bigint,
    getdate()   as etlchange
FROM
aws_fin_stage1_extracts.fin_stage2_gocardlesssubscriptions
;




select
	id  ,
	amount::bigint,
	amount_refunded::bigint,
	charge_date::timestamp,
	currency  ,
	description  ,
	reference  ,
	status  ,
	payout  ,
	mandate  ,
	subscription  ,
	ensekid  ,
	statementid,
    getdate()   as etlchange
from
aws_fin_stage1_extracts.fin_stage2_gocardlesspaymentstesting
;