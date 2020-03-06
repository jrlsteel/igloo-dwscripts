create or replace view vw_fin_gross_margin_journals_elec_msgsp as
select elec.*,
       gsp.gsp,
       coalesce(case when elec.gspgroupid = '' then null else elec.gspgroupid end, gsp.gsp) as gspgroupid_1
FROM aws_fin_stage1_extracts.fin_gross_margin_journals_elec elec
       left join (select accountid, gsp
                  from aws_fin_stage1_extracts.fin_gross_margin_missing_gsp
                  where gsp != '' or gsp is not null group by accountid, gsp) gsp on gsp.accountid = elec.accountid
    WITH NO SCHEMA BINDING
;

alter table vw_fin_gross_margin_journals_elec_msgsp owner to igloo
;

