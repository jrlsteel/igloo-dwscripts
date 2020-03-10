create or replace view vw_fin_gross_margin_missing_gsp as
select accountid, gsp, row_number() over (partition by accountid order by gsp) as rowid
                     from (select accountid, gsp
                           from aws_fin_stage1_extracts.fin_gross_margin_missing_gsp
                           where gsp != ''
                              or gsp is not null
                           group by accountid, gsp)
    WITH NO SCHEMA BINDING
;

alter table vw_fin_gross_margin_missing_gsp owner to igloo
;


