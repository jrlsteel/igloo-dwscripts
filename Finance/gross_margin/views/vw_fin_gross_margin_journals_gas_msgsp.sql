create or replace view vw_fin_gross_margin_journals_gas_msgsp as
select gas.*,
       gsp.gspgroupid,
       msgsp.gsp,
       coalesce(case
                  when (gsp.gspgroupid) = '' and (gas.accountid is not null)
                          then null
                  when (gas.accountid is null)
                          then '_H'
                  else gsp.gspgroupid
                    end, msgsp.gsp, dcf.gsp) as gspgroupid_1
from aws_fin_stage1_extracts.fin_gross_margin_journals_gas gas
       left join public.vw_fin_gross_margin_gas_gsp gsp on gas.accountid = gsp.accountid
       left join public.vw_fin_gross_margin_missing_gsp msgsp on gsp.accountid = msgsp.accountid
       left join public.ref_calculated_daily_customer_file dcf on dcf.account_id = gas.accountid
    WITH NO SCHEMA BINDING
;

alter table vw_fin_gross_margin_journals_gas_msgsp owner to igloo
;

