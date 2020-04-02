
; with cte_payment as (
--- PAYMENT REPORT ---
select
paym.id as paymentID ,
 paym.created_at,
 paym.charge_date,
  paym.amount,
  paym.status,
  paym.description,
  paym.statementid,
  client.client_id as customers_id ,
  coalesce(client.ensekid, cuser.ensek_id) as ensekAccountId
--- PAYMENTS ---
from aws_fin_stage1_extracts.fin_go_cardless_api_payments paym
--- MANDATES ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
    on man.mandate_id = paym.mandate
--- goCardless CLIENTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
    on client.client_id = man.customerid
--- ref_cdb_users ---
left join public.ref_cdb_users cuser
   on replace(cuser.email, ' ', '') = replace(client.email, ' ', '')
where man.mandate_id is not null
)


, cte_refunds as (
--- REFUNDS REPORT ---
SELECT
client.client_id  as customers_id ,
coalesce(client.ensekid, cuser.ensek_id) as ensekAccountId,
refu.id as refundsID,
refu.created_at,
pyot.arrival_date ,
refu.status,
refu.amount as refund_amount,
paym.amount_refunded ,
paym.amount as payment_amount ,
pyot.payout_type ,
paym.status as payment_status,
refu.payment as paymentID ,
refu.mandate ,
pyot.payout_id
--- REFUNDS ---
FROM aws_fin_stage1_extracts.fin_go_cardless_api_refunds refu
--- MANDATES ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
    on man.mandate_id = refu.mandate
--- goCardless CLIENTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
    on client.client_id = man.customerid
--- ref_cdb_users ---
left join public.ref_cdb_users cuser
   on replace(cuser.email, ' ', '') = replace(client.email, ' ', '')
--- PAYMENTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_payments paym
    on paym.id = refu.payment
--- PAYOUTS ---
left join aws_fin_stage1_extracts.fin_go_cardless_api_payouts pyot
    on pyot.payout_id = paym.payout
 where man.mandate_id is not null
       )


select
       (sum(Case when pay.ensekAccountId is not null then 1 else 0 end )::float / count(* )) * 100  as  "PERC_ensekAccountId" ,
       (sum(Case when pay.customers_id is not null then 1 else 0 end )::float / count(* )) * 100  as  "PERC_customers_id"


from cte_payment pay;