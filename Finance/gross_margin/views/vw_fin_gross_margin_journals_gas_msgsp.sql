create or replace view vw_fin_gross_margin_journals_gas_msgsp as

select *
from (select gas.*,
             cast(gas.WholesaleCost as float)                    as WholesaleCost_dec,
             gsp_1.gspgroupid,
             msgsp.gsp,
             coalesce(case
                        when (gsp_1.gspgroupid) = '' and (gas.accountid is not null)
                                then null
                        when (gas.accountid is null)
                                then '_H'
                        else gsp_1.gspgroupid
                          end, msgsp.gsp, dcf.gsp, gsp_link.gsp) as gspgroupid_1
      from aws_fin_stage1_extracts.fin_gross_margin_journals_gas gas
             left join public.vw_fin_gross_margin_gas_gsp gsp_1 on gas.accountid = gsp_1.accountid
             left join public.vw_fin_gross_margin_missing_gsp msgsp on gsp_1.accountid = msgsp.accountid and msgsp.rowid = 1
             left join public.ref_calculated_daily_customer_file dcf on dcf.account_id = gas.accountid
             left join (select accountid, gsp
                        from aws_fin_stage1_extracts.fin_gross_margin_missing_gsp
                        where gsp != ''
          or gsp is not null group by accountid, gsp) gsp on gsp.accountid = gas.accountid
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
          group by mp.account_id) gsp_link on gsp_link.account_id = gas.accountid --and gas.accountid is not null
     ) gas_view
    WITH NO SCHEMA BINDING
;

alter table vw_fin_gross_margin_journals_gas_msgsp owner to igloo
;

