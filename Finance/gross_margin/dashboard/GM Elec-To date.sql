select gspgroupid,
       TheoreticalRevenue,
       TheoreticalStandingCharge,
       Billed_Revenue,
       BilledStandingCharge,
       WholesaleCost,
       rcrccost,
       bsuoscost,
       tnuoscost,
       duoscommoditycost,
       duoscapacitycost,
       aahedccost,
       robuyoutcost,
       cfdoperationalcost,
       cfdinterimcost,
       cfdrecadj,
       rowcount
from (select gspgroupid_1                   as gspgroupid,
             SUM(TheoreticalRevenue)        as TheoreticalRevenue,
             SUM(TheoreticalStandingCharge) as TheoreticalStandingCharge,
             SUM(BilledRevenue)             as Billed_Revenue,
             SUM(BilledStandingCharge)      as BilledStandingCharge,
             SUM(WholesaleCost)             as WholesaleCost,
             SUM(rcrccost)                  as rcrccost,
             SUM(bsuoscost)                 as bsuoscost,
             SUM(tnuoscost)                 as tnuoscost,
             SUM(duoscommoditycost)         as duoscommoditycost,
             SUM(duoscapacitycost)          as duoscapacitycost,
             SUM(aahedccost)                as aahedccost,
             SUM(robuyoutcost)              as robuyoutcost,
             SUM(cfdoperationalcost)        as cfdoperationalcost,
             SUM(cfdinterimcost)            as cfdinterimcost,
             SUM(cfdrecadj)                 as cfdrecadj,
             count(*)                       as rowcount,
             0                              as key
      FROM vw_fin_gross_margin_journals_elec_msgsp elec
          --where consumptionmonth = '2020-01-01'
      where timestamp = '$ReportMonth'
      group by gspgroupid_1

      UNION

      select 'Total' as gspgroupid, stg_Tots.*, 1 as key
      from (select --'Total'                  as gspgroupid,
                   SUM(TheoreticalRevenue)        as TheoreticalRevenue,
                   SUM(TheoreticalStandingCharge) as TheoreticalStandingCharge,
                   SUM(BilledRevenue)             as Billed_Revenue,
                   SUM(BilledStandingCharge)      as BilledStandingCharge,
                   SUM(WholesaleCost)             as WholesaleCost,
                   SUM(rcrccost)                  as rcrccost,
                   SUM(bsuoscost)                 as bsuoscost,
                   SUM(tnuoscost)                 as tnuoscost,
                   SUM(duoscommoditycost)         as duoscommoditycost,
                   SUM(duoscapacitycost)          as duoscapacitycost,
                   SUM(aahedccost)                as aahedccost,
                   SUM(robuyoutcost)              as robuyoutcost,
                   SUM(cfdoperationalcost)        as cfdoperationalcost,
                   SUM(cfdinterimcost)            as cfdinterimcost,
                   SUM(cfdrecadj)                 as cfdrecadj,
                   count(*)                       as rowcount
            FROM vw_fin_gross_margin_journals_elec_msgsp elec
          --where consumptionmonth = '2020-01-01'
            where timestamp = '$ReportMonth'
           ) stg_Tots)tots
order by key, gspgroupid;


