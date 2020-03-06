--- Square Payments ---

 select gspgroupid, c_7001, c_7604, c_8040, c_8041
    from (SELECT
              stg2.*,
               (c_7001 - (c_7001/1.05)) as c_8040,
               ((c_7001 - (c_7001/1.05)) * -1) as c_8041,
              0 as key
              from
                  (
                    select
                    stg_1.gspgroupid,
                    (c_7604 * -1) as c_7001,
                     stg_1.c_7604
                     from
                          (
                          select
                          gspgroupid_1 as gspgroupid,
                          SUM(CASE WHEN (Nominal = 7604) THEN TransAmount ELSE 0 END) as c_7604
                          from
                          vw_fin_sales_ledger_journals
                          where timestamp = '$ReportMonth'
                          group by gspgroupid_1
                          order by gspgroupid_1
                          ) stg_1
                        )stg2


UNION

      -- Totals --
      select 'Total'     as gspgroupid,
             sum(c_7001) as c_7001,
             sum(c_7604) as c_7604,
             sum(c_8040) as c_8040,
             sum(c_8041) as c_8041,
             1           as key
      from (SELECT
              stg2.*,
               (c_7001 - (c_7001/1.05)) as c_8040,
               ((c_7001 - (c_7001/1.05)) * -1) as c_8041
              from
                  (
                    select
                    stg_1.gspgroupid,
                    (c_7604 * -1) as c_7001,
                     stg_1.c_7604
                     from
                          (
                          select
                          gspgroupid_1 as gspgroupid,
                          SUM(CASE WHEN (Nominal = 7604) THEN TransAmount ELSE 0 END) as c_7604
                          from
                          vw_fin_sales_ledger_journals
                          where timestamp = '$ReportMonth'
                          group by gspgroupid_1
                          order by gspgroupid_1
                          ) stg_1
                        )stg2
                ) stg_Tots )tots
order by key, gspgroupid;