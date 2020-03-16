--- MISC Charges @5% VAT ---
SELECT
gspgroupid, c_7001, c_2610, c_8041
FROM (
        select
        stg_1.gspgroupid,
        (stg_1.c_2610 * -1.05) as c_7001,
        stg_1.c_2610,
        ((stg_1.c_2610 * -1.05) * -1) - stg_1.c_2610 as c_8041,
        0 as key
        from
                    (
                       select
                       gspgroupid_1 as gspgroupid,
                       (SUM(CASE WHEN (Nominal = 2610) THEN TransAmount ELSE 0 END) / 1.05) as c_2610
                       from
                       vw_fin_sales_ledger_journals
                       where timestamp = '$ReportMonth'
                       group by gspgroupid_1
                       order by gspgroupid_1
                    ) stg_1

UNION


select 'Total' as gspgroupid, sum(c_7001) as c_7001, sum(c_2610) as c_2610, sum(c_8041) as c_8041, 1 as key
      FROM (select stg_1.gspgroupid,
                   (stg_1.c_2610 * -1.05)                       as c_7001,
                   stg_1.c_2610,
                   ((stg_1.c_2610 * -1.05) * -1) - stg_1.c_2610 as c_8041,
                   0                                            as key
            from (select gspgroupid_1                                                         as gspgroupid,
                         (SUM(CASE WHEN (Nominal = 2610) THEN TransAmount ELSE 0 END) / 1.05) as c_2610
                  from vw_fin_sales_ledger_journals
                  where timestamp = '$ReportMonth'
                  group by gspgroupid_1
                  order by gspgroupid_1) stg_1)stg_tots
)tots
order by key, gspgroupid ;