--- GSP Exceptions - Elec - Current ---
select
distinct mpan, gspgroupid_1 as gsp
FROM vw_fin_gross_margin_journals_elec_msgsp
  where consumptionmonth = '$ReportMonth'
    and timestamp = '$ReportMonth'
    and gspgroupid_1 is null or gspgroupid_1 = ''
;
