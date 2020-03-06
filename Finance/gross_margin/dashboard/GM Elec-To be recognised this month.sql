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
       cfdrecadj
from (select a.gspgroupid,
                   a.TheoreticalRevenue - b.TheoreticalRevenue               as "TheoreticalRevenue",
                   a.TheoreticalStandingCharge - b.TheoreticalStandingCharge as "TheoreticalStandingCharge",
                   a.Billed_Revenue - b.Billed_Revenue                       as "Billed_Revenue",
                   a.BilledStandingCharge - b.BilledStandingCharge           as "BilledStandingCharge",
                   a.WholesaleCost - b.WholesaleCost                         as "WholesaleCost",
                   a.rcrccost - b.rcrccost                                   as "rcrccost",
                   a.bsuoscost - b.bsuoscost                                 as "bsuoscost",
                   a.tnuoscost - b.tnuoscost                                 as "tnuoscost",
                   a.duoscommoditycost - b.duoscommoditycost                 as "duoscommoditycost",
                   a.duoscapacitycost - b.duoscapacitycost                   as "duoscapacitycost",
                   a.aahedccost - b.aahedccost                               as "aahedccost",
                   a.robuyoutcost - b.robuyoutcost                           as "robuyoutcost",
                   a.cfdoperationalcost - b.cfdoperationalcost               as "cfdoperationalcost",
                   a.cfdinterimcost - b.cfdinterimcost                       as "cfdinterimcost",
                   a.cfdrecadj - b.cfdrecadj                                 as "cfdrecadj",
                   0                                                         as key
            from
          --A - To date = latest file all months
                 (select a1.gspgroupid_1                   as gspgroupid,
                         SUM(a1.TheoreticalRevenue)        as TheoreticalRevenue,
                         SUM(a1.TheoreticalStandingCharge) as TheoreticalStandingCharge,
                         SUM(a1.BilledRevenue)             as Billed_Revenue,
                         SUM(a1.BilledStandingCharge)      as BilledStandingCharge,
                         SUM(a1.WholesaleCost)             as WholesaleCost,
                         SUM(a1.rcrccost)                  as rcrccost,
                         SUM(a1.bsuoscost)                 as bsuoscost,
                         SUM(a1.tnuoscost)                 as tnuoscost,
                         SUM(a1.duoscommoditycost)         as duoscommoditycost,
                         SUM(a1.duoscapacitycost)          as duoscapacitycost,
                         SUM(a1.aahedccost)                as aahedccost,
                         SUM(a1.robuyoutcost)              as robuyoutcost,
                         SUM(a1.cfdoperationalcost)        as cfdoperationalcost,
                         SUM(a1.cfdinterimcost)            as cfdinterimcost,
                         SUM(a1.cfdrecadj)                 as cfdrecadj
                  FROM vw_fin_gross_margin_journals_elec_msgsp a1
                     --where consumptionmonth = '2020-01-01'
                  where a1.timestamp = '$ReportMonth'
                  group by a1.gspgroupid_1
                  order by a1.gspgroupid_1) a
                   inner join -- B -To date previous = previous file all months
                     (select b1.gspgroupid_1                   as gspgroupid,
                             SUM(b1.TheoreticalRevenue)        as TheoreticalRevenue,
                             SUM(b1.TheoreticalStandingCharge) as TheoreticalStandingCharge,
                             SUM(b1.BilledRevenue)             as Billed_Revenue,
                             SUM(b1.BilledStandingCharge)      as BilledStandingCharge,
                             SUM(b1.WholesaleCost)             as WholesaleCost,
                             SUM(b1.rcrccost)                  as rcrccost,
                             SUM(b1.bsuoscost)                 as bsuoscost,
                             SUM(b1.tnuoscost)                 as tnuoscost,
                             SUM(b1.duoscommoditycost)         as duoscommoditycost,
                             SUM(b1.duoscapacitycost)          as duoscapacitycost,
                             SUM(b1.aahedccost)                as aahedccost,
                             SUM(b1.robuyoutcost)              as robuyoutcost,
                             SUM(b1.cfdoperationalcost)        as cfdoperationalcost,
                             SUM(b1.cfdinterimcost)            as cfdinterimcost,
                             SUM(b1.cfdrecadj)                 as cfdrecadj
                      FROM vw_fin_gross_margin_journals_elec_msgsp  b1
                         --where consumptionmonth = '2020-01-01'
                      where b1.timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
                      group by b1.gspgroupid_1
                      order by b1.gspgroupid_1) b on a.gspgroupid = b.gspgroupid


 UNION

      select 'Total' as gspgroupid, stg_Tots.*, 1 as key
      from (select --'Total'                  as gspgroupid,
                   SUM(TheoreticalRevenue)        as TheoreticalRevenue,
                   SUM(TheoreticalStandingCharge) as TheoreticalStandingCharge,
                   SUM(Billed_Revenue)             as Billed_Revenue,
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
                   SUM(cfdrecadj)                 as cfdrecadj
         from   (
              select a.gspgroupid,
                   a.TheoreticalRevenue - b.TheoreticalRevenue               as "TheoreticalRevenue",
                   a.TheoreticalStandingCharge - b.TheoreticalStandingCharge as "TheoreticalStandingCharge",
                   a.Billed_Revenue - b.Billed_Revenue                       as "Billed_Revenue",
                   a.BilledStandingCharge - b.BilledStandingCharge           as "BilledStandingCharge",
                   a.WholesaleCost - b.WholesaleCost                         as "WholesaleCost",
                   a.rcrccost - b.rcrccost                                   as "rcrccost",
                   a.bsuoscost - b.bsuoscost                                 as "bsuoscost",
                   a.tnuoscost - b.tnuoscost                                 as "tnuoscost",
                   a.duoscommoditycost - b.duoscommoditycost                 as "duoscommoditycost",
                   a.duoscapacitycost - b.duoscapacitycost                   as "duoscapacitycost",
                   a.aahedccost - b.aahedccost                               as "aahedccost",
                   a.robuyoutcost - b.robuyoutcost                           as "robuyoutcost",
                   a.cfdoperationalcost - b.cfdoperationalcost               as "cfdoperationalcost",
                   a.cfdinterimcost - b.cfdinterimcost                       as "cfdinterimcost",
                   a.cfdrecadj - b.cfdrecadj                                 as "cfdrecadj"

            from
          --A - To date = latest file all months
                 (select a1.gspgroupid_1                   as gspgroupid,
                         SUM(a1.TheoreticalRevenue)        as TheoreticalRevenue,
                         SUM(a1.TheoreticalStandingCharge) as TheoreticalStandingCharge,
                         SUM(a1.BilledRevenue)             as Billed_Revenue,
                         SUM(a1.BilledStandingCharge)      as BilledStandingCharge,
                         SUM(a1.WholesaleCost)             as WholesaleCost,
                         SUM(a1.rcrccost)                  as rcrccost,
                         SUM(a1.bsuoscost)                 as bsuoscost,
                         SUM(a1.tnuoscost)                 as tnuoscost,
                         SUM(a1.duoscommoditycost)         as duoscommoditycost,
                         SUM(a1.duoscapacitycost)          as duoscapacitycost,
                         SUM(a1.aahedccost)                as aahedccost,
                         SUM(a1.robuyoutcost)              as robuyoutcost,
                         SUM(a1.cfdoperationalcost)        as cfdoperationalcost,
                         SUM(a1.cfdinterimcost)            as cfdinterimcost,
                         SUM(a1.cfdrecadj)                 as cfdrecadj
                  FROM vw_fin_gross_margin_journals_elec_msgsp a1
                     --where consumptionmonth = '2020-01-01'
                  where a1.timestamp = '$ReportMonth'
                  group by a1.gspgroupid_1
                  order by a1.gspgroupid_1) a
                   inner join -- B -To date previous = previous file all months
                     (select b1.gspgroupid_1                   as gspgroupid,
                             SUM(b1.TheoreticalRevenue)        as TheoreticalRevenue,
                             SUM(b1.TheoreticalStandingCharge) as TheoreticalStandingCharge,
                             SUM(b1.BilledRevenue)             as Billed_Revenue,
                             SUM(b1.BilledStandingCharge)      as BilledStandingCharge,
                             SUM(b1.WholesaleCost)             as WholesaleCost,
                             SUM(b1.rcrccost)                  as rcrccost,
                             SUM(b1.bsuoscost)                 as bsuoscost,
                             SUM(b1.tnuoscost)                 as tnuoscost,
                             SUM(b1.duoscommoditycost)         as duoscommoditycost,
                             SUM(b1.duoscapacitycost)          as duoscapacitycost,
                             SUM(b1.aahedccost)                as aahedccost,
                             SUM(b1.robuyoutcost)              as robuyoutcost,
                             SUM(b1.cfdoperationalcost)        as cfdoperationalcost,
                             SUM(b1.cfdinterimcost)            as cfdinterimcost,
                             SUM(b1.cfdrecadj)                 as cfdrecadj
                      FROM vw_fin_gross_margin_journals_elec_msgsp  b1
                         --where consumptionmonth = '2020-01-01'
                      where b1.timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
                      group by b1.gspgroupid_1
                      order by b1.gspgroupid_1) b on a.gspgroupid = b.gspgroupid ) stg
           ) stg_Tots)tots
order by key, gspgroupid;