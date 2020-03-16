-- GSP Exceptions - Gas - Previous month to date ---
select
 distinct mpr as mpan, gspgroupid_1 as gsp
  FROM vw_fin_gross_margin_journals_gas_msgsp gas
  WHERE timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
  and gspgroupid_1 is null or gspgroupid_1 = ''
;