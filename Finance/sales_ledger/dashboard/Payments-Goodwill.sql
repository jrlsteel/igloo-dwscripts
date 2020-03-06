--- Goodwill (just adding credit to an account) ---
select gspgroupid, c_7001, c_3200
FROM
(
          select
          stg_1.gspgroupid,
          (stg_1.c_3200 * -1) as c_7001,
          stg_1.c_3200,
          0 as key
          from
                      (
                         select
                         gspgroupid_1 as gspgroupid,
                         SUM(CASE WHEN (Nominal = 3200) THEN TransAmount ELSE 0 END)  as c_3200
                         from
                         vw_fin_sales_ledger_journals
                         where timestamp = '$ReportMonth'
                         group by gspgroupid_1
                         order by gspgroupid_1
                      ) stg_1


UNION

      select 'Total' as gspgroupid, sum(c_7001) as c_7001, sum(c_3200) as c_3200, 1 as key
      FROM
              (select stg_1.gspgroupid, (stg_1.c_3200 * -1) as c_7001, stg_1.c_3200
               from (select gspgroupid_1                                                as gspgroupid,
                            SUM(CASE WHEN (Nominal = 3200) THEN TransAmount ELSE 0 END) as c_3200
                     from vw_fin_sales_ledger_journals
                     where timestamp = '$ReportMonth'
                     group by gspgroupid_1
                     order by gspgroupid_1) stg_1
              ) stg_tots
  )tots
order by key, gspgroupid ;