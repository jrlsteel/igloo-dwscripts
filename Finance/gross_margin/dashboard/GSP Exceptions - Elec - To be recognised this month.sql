--- GSP Exceptions - Elec - To be recognised this month ----
select
distinct mpan, gspgroupid_1 as gsp
        FROM vw_fin_gross_margin_journals_elec_msgsp a1
           --where consumptionmonth = '2020-01-01'
        where a1.timestamp = '$ReportMonth'
           and gspgroupid_1 is null or gspgroupid_1 = ''

 UNION

  select
distinct mpan, gspgroupid_1 as gsp
            FROM vw_fin_gross_margin_journals_elec_msgsp  b1
               --where consumptionmonth = '2020-01-01'
            where b1.timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
           and gspgroupid_1 is null or gspgroupid_1 = ''
;