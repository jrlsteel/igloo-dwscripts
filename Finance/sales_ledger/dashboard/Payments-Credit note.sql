--- Credit note (reversing incorrect elec/gas bills) ---
select gspgroupid, c_7001, c_7010, c_7020, c_8041
FROM
  (
        select
        gspgroupid_1 as gspgroupid,
        SUM(CASE WHEN (Nominal = 7001 and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END) as c_7001,
        --SUM(CASE WHEN (Nominal = 2000 and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END)  +
        SUM(CASE WHEN (Nominal in (2000, 2003) and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END)   as c_7010,
        SUM(CASE WHEN (Nominal in (2100, 2103) and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END)   as c_7020,
        SUM(CASE WHEN (Nominal = 8040 and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END)   as c_8041,
        0 as key
        from
        vw_fin_sales_ledger_journals
        where timestamp = '$ReportMonth'
        group by gspgroupid_1


UNION

        select 'Total' as gspgroupid, sum(c_7001) as c_7001, sum(c_7010) as c_7010, sum(c_7020) as c_7020, sum(c_8041) as c_8041, 1 as key
        FROM
            (
             select
                gspgroupid_1 as gspgroupid,
                SUM(CASE WHEN (Nominal = 7001 and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END) as c_7001,
                --SUM(CASE WHEN (Nominal = 2000 and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END)  +
                SUM(CASE WHEN (Nominal in (2000, 2003) and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END)   as c_7010,
                SUM(CASE WHEN (Nominal in (2100, 2103) and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END)   as c_7020,
                SUM(CASE WHEN (Nominal = 8040 and lower(TransactionTypeName) = 'credit note') THEN TransAmount ELSE 0 END)   as c_8041
                from
                vw_fin_sales_ledger_journals
                where timestamp = '$ReportMonth'
                group by gspgroupid_1
            ) stg_tots
  )tots
order by key, gspgroupid ;