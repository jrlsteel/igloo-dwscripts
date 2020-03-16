select
gspgroupid,
c_7001,
c_3016
from
(
select
stg_1.gspgroupid,
(stg_1.c_3016 * -1) as c_7001,
stg_1.c_3016,
0 as key
from
            (
               select
               gspgroupid_1 as gspgroupid,
               SUM(CASE WHEN (Nominal = 3016) THEN TransAmount ELSE 0 END)  as c_3016
               from
               vw_fin_sales_ledger_journals
               where timestamp = '$ReportMonth'
               group by gspgroupid_1
               order by gspgroupid_1
            ) stg_1


UNION ALL

-- Totals --
select
'Total' as gspgroupid,
sum(c_7001) as c_7001,
sum(c_3016) as c_3016,
1 as key
from
     (
        select
               stg_1.gspgroupid,
               (stg_1.c_3016 * -1) as c_7001,
               stg_1.c_3016
               from
                           (
                              select
                              gspgroupid_1 as gspgroupid,
                              SUM(CASE WHEN (Nominal = 3016) THEN TransAmount ELSE 0 END)  as c_3016
                              from
                              vw_fin_sales_ledger_journals
                              where timestamp = '$ReportMonth'
                              group by gspgroupid_1
                              order by gspgroupid_1
                           ) stg_1
         )
)
order by key, gspgroupid ;
