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
                    end, msgsp.gsp, dcf.gsp, gsp_link.gsp) as gspgroupid_1
from aws_fin_stage1_extracts.fin_sales_ledger_journals ledger
       left join public.vw_fin_gross_margin_gas_gsp gsp on ledger.accountid = gsp.accountid
       left join public.vw_fin_gross_margin_missing_gsp msgsp on gsp.accountid = msgsp.accountid and msgsp.rowid = 1
       left join public.ref_calculated_daily_customer_file dcf on dcf.account_id = ledger.accountid
       left join (select mp.account_id, max(rma_gsp.attributes_attributevalue) as gsp
                  from public.ref_meterpoints_raw mp
                         inner join public.ref_meterpoints_raw prev_at_address
                           on mp.meter_point_id = prev_at_address.meter_point_id
                         inner join public.ref_meterpoints_raw address_linked_elec
                           on prev_at_address.account_id = address_linked_elec.account_id and
                              address_linked_elec.meterpointtype = 'E'
                         inner join public.ref_meterpoints_attributes rma_gsp
                           on rma_gsp.meter_point_id = address_linked_elec.meter_point_id and
                              rma_gsp.attributes_attributename ilike 'gsp'
    group by mp.account_id) gsp_link on gsp_link.account_id = ledger.accountid
    WITH NO SCHEMA BINDING
;

alter table vw_fin_sales_ledger_journals owner to igloo
;

