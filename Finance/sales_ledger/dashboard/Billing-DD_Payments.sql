--- DD Payments ---
select gspgroupid, c_7001, c_7603, c_8040, c_8041
from (select stg_1.*, (c_7001 - (c_7001 / 1.05)) as c_8040, ((c_7001 - (c_7001 / 1.05)) * -1) as c_8041, 0 as key
      from (select gspgroupid_1      as gspgroupid,
                   SUM(CASE
                         WHEN (Nominal = 7001 and TransactionTypeName = 'Payment') THEN TransAmount
                         ELSE 0 END) as c_7001,
                   SUM(CASE
                         WHEN (Nominal = 7603 and TransactionTypeName = 'Payment') THEN TransAmount
                         ELSE 0 END) as c_7603
            from vw_fin_sales_ledger_journals
            where timestamp = '$ReportMonth'
            group by gspgroupid_1
            order by gspgroupid_1) stg_1


      UNION

      -- Totals --
      select 'Total'     as gspgroupid,
             sum(c_7001) as c_7001,
             sum(c_7603) as c_7603,
             sum(c_8040) as c_8040,
             sum(c_8041) as c_8041,
             1           as key
      from (select stg_1.*, (c_7001 - (c_7001 / 1.05)) as c_8040, ((c_7001 - (c_7001 / 1.05)) * -1) as c_8041
            from (select gspgroupid_1      as gspgroupid,
                         SUM(CASE
                               WHEN (Nominal = 7001 and TransactionTypeName = 'Payment') THEN TransAmount
                               ELSE 0 END) as c_7001,
                         SUM(CASE
                               WHEN (Nominal = 7603 and TransactionTypeName = 'Payment') THEN TransAmount
                               ELSE 0 END) as c_7603
                  from vw_fin_sales_ledger_journals
                  where timestamp = '$ReportMonth'
                  group by gspgroupid_1
                  order by gspgroupid_1) stg_1) stg_Tots)tots
order by key, gspgroupid;