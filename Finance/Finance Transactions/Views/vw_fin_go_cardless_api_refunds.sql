create or replace view vw_fin_go_cardless_api_refunds as

--- REFUNDS REPORT ---
SELECT client.client_id                         as customers_id,
       coalesce(client.ensekid, cuser.ensek_id) as ensekAccountId,
       refu.id                                  as refundsID,
       refu.created_at,
       pyot.arrival_date,
       refu.status,
       refu.amount::float / 100 as refund_amount,
       paym.amount_refunded::float / 100 as amount_refunded,
       paym.amount::float / 100 as payment_amount,
       pyot.payout_type,
       paym.status                              as payment_status,
       refu.payment                             as paymentID,
       refu.mandate,
       pyot.payout_id
    --- REFUNDS ---
FROM aws_fin_stage1_extracts.fin_go_cardless_api_refunds refu --- MANDATES ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
         on man.mandate_id = refu.mandate --- goCardless CLIENTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
         on client.client_id = man.customerid --- ref_cdb_users ---
       left join public.ref_cdb_users cuser
         on replace(cuser.email, ' ', '') = replace(client.email, ' ', '') --- PAYMENTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_payments paym on paym.id = refu.payment --- PAYOUTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_payouts pyot on pyot.payout_id = paym.payout
where man.mandate_id is not null
    WITH NO SCHEMA BINDING
;

alter table vw_fin_go_cardless_api_refunds owner to igloo
;

