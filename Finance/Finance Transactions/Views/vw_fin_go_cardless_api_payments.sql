create or replace view vw_fin_go_cardless_api_payments as

--- PAYMENT REPORT ---
select paym.id                                  as paymentID,
       paym.created_at,
       paym.charge_date,
       paym.amount::float / 100 as amount,
       paym.status,
       paym.description,
       paym.statementid,
       client.client_id                         as customers_id,
       coalesce(client.ensekid, cuser.ensek_id) as ensekAccountId
    --- PAYMENTS ---
from aws_fin_stage1_extracts.fin_go_cardless_api_payments paym --- MANDATES ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
         on man.mandate_id = paym.mandate --- goCardless CLIENTS ---
       left join aws_fin_stage1_extracts.fin_go_cardless_api_clients client
         on client.client_id = man.customerid --- ref_cdb_users ---
       left join public.ref_cdb_users cuser on replace(cuser.email, ' ', '') = replace(client.email, ' ', '')
where man.mandate_id is not null
    WITH NO SCHEMA BINDING
;

alter table vw_fin_go_cardless_api_payments owner to igloo
;