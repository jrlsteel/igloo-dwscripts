create or replace view vw_fin_gross_margin_gas_gsp as
select accountid, gspgroupid
from (select elec.accountid, rtrim(ltrim(elec.gspgroupid)) as gspgroupid
      from aws_fin_stage1_extracts.fin_gross_margin_journals_elec elec
      where timestamp <= (select max (timestamp) from aws_fin_stage1_extracts.fin_gross_margin_journals_elec elec)
    group by elec.accountid, elec.gspgroupid)
where accountid is not null
  and gspgroupid != ''
    WITH NO SCHEMA BINDING
;

alter table vw_fin_gross_margin_gas_gsp owner to igloo
;

