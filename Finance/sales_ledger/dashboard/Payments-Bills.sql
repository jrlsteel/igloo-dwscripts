select gspgroupid, c_7001, c_7010, c_8041
from
(select gspgroupid_1      as gspgroupid,
             SUM(CASE
                   WHEN (Nominal = 7001 and lower(TransactionTypeName) = 'bill') THEN TransAmount
                   ELSE 0 END) as c_7001,
             SUM(CASE
                   WHEN (Nominal < 2100 and lower(TransactionTypeName) = 'bill') THEN TransAmount
                   ELSE 0 END) as c_7010,
             SUM(CASE
                   WHEN ((Nominal between 2099 and 2200) and lower(TransactionTypeName) = 'bill') THEN TransAmount
                   ELSE 0 END) as c_7020,
             SUM(CASE
                   WHEN (Nominal = 8040 and lower(TransactionTypeName) = 'bill') THEN TransAmount
                   ELSE 0 END) as c_8041,
             0                 as key
      from vw_fin_sales_ledger_journals
      where timestamp = '$ReportMonth'
      group by gspgroupid_1


 UNION


 -- Totals --
      select 'Total'     as gspgroupid,
             sum(c_7001) as c_7001,
             sum(c_7010) as c_7010,
             sum(c_7020) as c_7020,
             sum(c_8041) as c_8041,
             1           as key
      from (Select gspgroupid_1      as gspgroupid,
                   SUM(CASE
                         WHEN (Nominal = 7001 and lower(TransactionTypeName) = 'bill') THEN TransAmount
                         ELSE 0 END) as c_7001,
                   SUM(CASE
                         WHEN (Nominal < 2100 and lower(TransactionTypeName) = 'bill') THEN TransAmount
                         ELSE 0 END) as c_7010,
                   SUM(CASE
                         WHEN ((Nominal between 2099 and 2200) and lower(TransactionTypeName) = 'bill') THEN TransAmount
                         ELSE 0 END) as c_7020,
                   SUM(CASE
                         WHEN (Nominal = 8040 and lower(TransactionTypeName) = 'bill') THEN TransAmount
                         ELSE 0 END) as c_8041
            from vw_fin_sales_ledger_journals
            where timestamp = '$ReportMonth'
            group by gspgroupid_1
            order by gspgroupid_1) tot_1
)tots
order by key, gspgroupid ;