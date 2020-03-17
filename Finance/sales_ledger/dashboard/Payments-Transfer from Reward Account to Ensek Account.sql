--- Transfer from Reward Account to Ensek Account ---
select gspgroupid, c_7001, c_7002
FROM
  (
          select
          stg_1.gspgroupid,
          (stg_1.c_7002 * -1) as c_7001,
          stg_1.c_7002,
          0 as key
          from
                      (
                         select
                         gspgroupid_1 as gspgroupid,
                         SUM(CASE WHEN (Nominal = 7002) THEN TransAmount ELSE 0 END)  as c_7002
                         from
                         vw_fin_sales_ledger_journals
                         where timestamp = '$ReportMonth'
                         group by gspgroupid_1
                         order by gspgroupid_1
                      ) stg_1


UNION

          select 'Total' as gspgroupid, sum(c_7001) as c_7001, sum(c_7002 ) as c_7002 , 1 as key
           FROM
              (
               select
                  stg_1.gspgroupid,
                  (stg_1.c_7002 * -1) as c_7001,
                  stg_1.c_7002
                  from
                              (
                                 select
                                 gspgroupid_1 as gspgroupid,
                                 SUM(CASE WHEN (Nominal = 7002) THEN TransAmount ELSE 0 END)  as c_7002
                                 from
                                 vw_fin_sales_ledger_journals
                                 where timestamp = '$ReportMonth'
                                 group by gspgroupid_1
                                 order by gspgroupid_1
                              ) stg_1
              ) stg_tots
  )tots
order by key, gspgroupid ;