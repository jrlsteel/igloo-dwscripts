create or replace view vw_fin_gross_margin_missing_gsp as
select msgsp.accountid, msgsp.gsp
from aws_fin_stage1_extracts.fin_gross_margin_missing_gsp msgsp
group by msgsp.accountid, msgsp.gsp
    WITH NO SCHEMA BINDING
;

alter table vw_fin_gross_margin_missing_gsp owner to igloo
;

