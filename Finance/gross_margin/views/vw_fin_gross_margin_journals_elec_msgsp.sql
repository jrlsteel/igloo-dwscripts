
create or replace view vw_fin_gross_margin_journals_elec_msgsp as
select elec.*,
       gsp.gsp,
       coalesce(case
                  when (elec.gspgroupid) = '' and (elec.accountid is not null) then null
                  when (elec.accountid is null) then '_H'
                  else elec.gspgroupid end, gsp.gsp, gsp_link.gsp) as gspgroupid_1

FROM aws_fin_stage1_extracts.fin_gross_margin_journals_elec elec
       left join public.vw_fin_gross_margin_missing_gsp gsp on gsp.accountid = elec.accountid and gsp.rowid = 1
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
    group by mp.account_id) gsp_link on gsp_link.account_id = elec.accountid and elec.accountid is not null
    WITH NO SCHEMA BINDING
;

alter table vw_fin_gross_margin_journals_elec_msgsp owner to igloo
;



