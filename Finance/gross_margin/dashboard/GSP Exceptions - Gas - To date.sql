--- GSP Exceptions - Gas - To date ---
   select
distinct mpr as mpan, gspgroupid_1 as gsp
FROM vw_fin_gross_margin_journals_gas_msgsp gas
  WHERE timestamp = '$ReportMonth'
                and gspgroupid_1 is null or gspgroupid_1 = ''
  ;