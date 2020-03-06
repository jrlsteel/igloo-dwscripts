-- Interest ---
select gspgroupid, c_7001, c_2800
from
(
Select stg.gspgroupid, stg.c_7001, (stg.c_7001 * -1) as c_2800, 0 as key
 from (select gspgroupid_1 as gspgroupid, SUM(CASE WHEN (Nominal = 2800) THEN TransAmount ELSE 0 END) as c_7001
       from vw_fin_sales_ledger_journals
       where timestamp = '$ReportMonth'
       group by gspgroupid_1
       order by gspgroupid_1) stg

 UNION

 select 'Total'     as gspgroupid,
        sum(c_7001) as c_7001,
        sum(c_2800) as c_2800,
        1 as key
 from (Select stg.gspgroupid, stg.c_7001, (stg.c_7001 * -1) as c_2800
       from (select gspgroupid_1 as gspgroupid, SUM(CASE WHEN (Nominal = 2800) THEN TransAmount ELSE 0 END) as c_7001
             from vw_fin_sales_ledger_journals
             where timestamp = '$ReportMonth'
             group by gspgroupid_1
             order by gspgroupid_1) stg)stg_tots
)tots
order by key, gspgroupid ;