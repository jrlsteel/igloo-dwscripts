create or replace view vw_fin_sales_ledger_journals as
select ledger.*,
       gsp.gspgroupid,
       msgsp.gsp,
       coalesce(case
                  when (gsp.gspgroupid) = '' and (ledger.accountid is not null)
                          then null
                  when (ledger.accountid is null)
                          then '_H'
                  else gsp.gspgroupid
                    end, msgsp.gsp, dcf.gsp) as gspgroupid_1
from aws_fin_stage1_extracts.fin_sales_ledger_journals ledger
       left join public.vw_fin_gross_margin_gas_gsp gsp on ledger.accountid = gsp.accountid
       left join public.vw_fin_gross_margin_missing_gsp msgsp on gsp.accountid = msgsp.accountid
       left join public.ref_calculated_daily_customer_file dcf on dcf.account_id = ledger.accountid
    WITH NO SCHEMA BINDING
;

alter table vw_fin_sales_ledger_journals owner to igloo
;

