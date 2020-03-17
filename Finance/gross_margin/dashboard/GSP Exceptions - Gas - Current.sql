--- GSP Exceptions - Gas - Current ----
 select
distinct mpr as mpan, gspgroupid_1 as gsp
  FROM vw_fin_gross_margin_journals_gas_msgsp gas
  where date (consumptionmonth) = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
  and timestamp = '$ReportMonth'
                and gspgroupid_1 is null or gspgroupid_1 = ''
;