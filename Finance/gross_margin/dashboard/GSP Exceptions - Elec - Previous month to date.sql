--- GSP Exceptions - Elec - Previous month to date ---
select
distinct mpan, gspgroupid_1 as gsp
FROM vw_fin_gross_margin_journals_elec_msgsp elec
      --where consumptionmonth = '2020-01-01'
  where timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
      and gspgroupid_1 is null or gspgroupid_1 = '';