--- GSP Exceptions - Gas - To be recognised this month ----
    select
distinct mpr as mpan, gspgroupid_1 as gsp
        FROM vw_fin_gross_margin_journals_gas_msgsp gas
        where timestamp = '$ReportMonth'
                and gspgroupid_1 is null or gspgroupid_1 = ''

UNION

    select
distinct mpr as mpan, gspgroupid_1 as gsp
        FROM vw_fin_gross_margin_journals_gas_msgsp gas
  WHERE timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
  and gspgroupid_1 is null or gspgroupid_1 = '' ;
