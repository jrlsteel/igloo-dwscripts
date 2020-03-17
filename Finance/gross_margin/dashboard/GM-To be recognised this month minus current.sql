select gspgroupid,
        TheoreticalRevenue,
        TheoreticalStandingCharge,
        BilledRevenue,
        ReprofiledChargeAmount,
        BilledStandingCharge,
        LDZCommodityCost,
        LDZCapacityCost,
        CustomerCapacityCost,
        CustomerFixedCost,
        NTSCommodityCost,
        LDZExitCapacityCost,
        WholesaleCost,
        BalancingFee,
        ManagementFee,
        Rec_TheoreticalRevenue,
        Rec_NTSCommodityCost,
        Rec_LDZCommodityCost,
        Rec_WholesaleCost
 from (
          select c.gspgroupid,
                       c.TheoreticalRevenue - d.TheoreticalRevenue               as "TheoreticalRevenue",
                       c.TheoreticalStandingCharge - d.TheoreticalStandingCharge as "TheoreticalStandingCharge",
                       c.BilledRevenue - d.BilledRevenue                         as "BilledRevenue",
                       c.ReprofiledChargeAmount - d.ReprofiledChargeAmount       as "ReprofiledChargeAmount",
                       c.BilledStandingCharge - d.BilledStandingCharge           as "BilledStandingCharge",
                       c.LDZCommodityCost - d.LDZCommodityCost                   as "LDZCommodityCost",
                       c.LDZCapacityCost - d.LDZCapacityCost                     as "LDZCapacityCost",
                       c.CustomerCapacityCost - d.CustomerCapacityCost           as "CustomerCapacityCost",
                       c.CustomerFixedCost - d.CustomerFixedCost                 as "CustomerFixedCost",
                       c.NTSCommodityCost - d.NTSCommodityCost                   as "NTSCommodityCost",
                       c.LDZExitCapacityCost - d.LDZExitCapacityCost             as "LDZExitCapacityCost",
                       c.WholesaleCost - d.WholesaleCost                         as "WholesaleCost",
                       c.BalancingFee - d.BalancingFee                           as "BalancingFee",
                       c.ManagementFee - d.ManagementFee                         as "ManagementFee",
                       c.Rec_TheoreticalRevenue - d.Rec_TheoreticalRevenue       as "Rec_TheoreticalRevenue",
                       c.Rec_NTSCommodityCost - d.Rec_NTSCommodityCost           as "Rec_NTSCommodityCost",
                       c.Rec_LDZCommodityCost - d.Rec_LDZCommodityCost           as "Rec_LDZCommodityCost",
                       c.Rec_WholesaleCost - d.Rec_WholesaleCost                 as "Rec_WholesaleCost",
		                    0                                  as key
                from (select a.gspgroupid,
                             a.TheoreticalRevenue - b.TheoreticalRevenue               as "TheoreticalRevenue",
                             a.TheoreticalStandingCharge - b.TheoreticalStandingCharge as "TheoreticalStandingCharge",
                             a.BilledRevenue - b.BilledRevenue                         as "BilledRevenue",
                             a.ReprofiledChargeAmount - b.ReprofiledChargeAmount       as "ReprofiledChargeAmount",
                             a.BilledStandingCharge - b.BilledStandingCharge           as "BilledStandingCharge",
                             a.LDZCommodityCost - b.LDZCommodityCost                   as "LDZCommodityCost",
                             a.LDZCapacityCost - b.LDZCapacityCost                     as "LDZCapacityCost",
                             a.CustomerCapacityCost - b.CustomerCapacityCost           as "CustomerCapacityCost",
                             a.CustomerFixedCost - b.CustomerFixedCost                 as "CustomerFixedCost",
                             a.NTSCommodityCost - b.NTSCommodityCost                   as "NTSCommodityCost",
                             a.LDZExitCapacityCost - b.LDZExitCapacityCost             as "LDZExitCapacityCost",
                             a.WholesaleCost - b.WholesaleCost                         as "WholesaleCost",
                             a.BalancingFee - b.BalancingFee                           as "BalancingFee",
                             a.ManagementFee - b.ManagementFee                         as "ManagementFee",
                             a.Rec_TheoreticalRevenue - b.Rec_TheoreticalRevenue       as "Rec_TheoreticalRevenue",
                             a.Rec_NTSCommodityCost - b.Rec_NTSCommodityCost           as "Rec_NTSCommodityCost",
                             a.Rec_LDZCommodityCost - b.Rec_LDZCommodityCost           as "Rec_LDZCommodityCost",
                             a.Rec_WholesaleCost - b.Rec_WholesaleCost                 as "Rec_WholesaleCost"
                      from (select gas.gspgroupid_1                   as gspgroupid,
                                   SUM(gas.TheoreticalRevenue)        as TheoreticalRevenue,
                                   SUM(gas.TheoreticalStandingCharge) as TheoreticalStandingCharge,
                                   SUM(gas.Billed_Revenue)            as BilledRevenue,
                                   SUM(gas.ReprofiledChargeAmount)    as ReprofiledChargeAmount,
                                   SUM(gas.BilledStandingCharge)      as BilledStandingCharge,
                                   SUM(gas.LDZCommodityCost)          as LDZCommodityCost,
                                   SUM(gas.LDZCapacityCost)           as LDZCapacityCost,
                                   SUM(gas.CustomerCapacityCost)      as CustomerCapacityCost,
                                   SUM(gas.CustomerFixedCost)         as CustomerFixedCost,
                                   SUM(gas.NTSCommodityCost)          as NTSCommodityCost,
                                   SUM(gas.LDZExitCapacityCost)       as LDZExitCapacityCost,
                                   SUM(gas.WholesaleCost)             as WholesaleCost,
                                   SUM(gas.BalancingFee)              as BalancingFee,
                                   SUM(gas.ManagementFee)             as ManagementFee,
                                   SUM(gas.reconciliation_revenue)    as Rec_TheoreticalRevenue,
                                   SUM(gas.AmendmentNTSCommodityCost) as Rec_NTSCommodityCost,
                                   SUM(gas.AmendmentLDZCommodityCost) as Rec_LDZCommodityCost,
                                   SUM(gas.AmendmentCommodityCharge)  as Rec_WholesaleCost,
                                   count(*)                           as rowcount
                            FROM vw_fin_gross_margin_journals_gas_msgsp gas
                            where timestamp = '$ReportMonth'
                            group by gas.gspgroupid_1
                            order by gas.gspgroupid_1) a
                             inner join (select gas.gspgroupid_1                   as gspgroupid,
                                                SUM(gas.TheoreticalRevenue)        as TheoreticalRevenue,
                                                SUM(gas.TheoreticalStandingCharge) as TheoreticalStandingCharge,
                                                SUM(gas.Billed_Revenue)            as BilledRevenue,
                                                SUM(gas.ReprofiledChargeAmount)    as ReprofiledChargeAmount,
                                                SUM(gas.BilledStandingCharge)      as BilledStandingCharge,
                                                SUM(gas.LDZCommodityCost)          as LDZCommodityCost,
                                                SUM(gas.LDZCapacityCost)           as LDZCapacityCost,
                                                SUM(gas.CustomerCapacityCost)      as CustomerCapacityCost,
                                                SUM(gas.CustomerFixedCost)         as CustomerFixedCost,
                                                SUM(gas.NTSCommodityCost)          as NTSCommodityCost,
                                                SUM(gas.LDZExitCapacityCost)       as LDZExitCapacityCost,
                                                SUM(gas.WholesaleCost)             as WholesaleCost,
                                                SUM(gas.BalancingFee)              as BalancingFee,
                                                SUM(gas.ManagementFee)             as ManagementFee,
                                                SUM(gas.reconciliation_revenue)    as Rec_TheoreticalRevenue,
                                                SUM(gas.AmendmentNTSCommodityCost) as Rec_NTSCommodityCost,
                                                SUM(gas.AmendmentLDZCommodityCost) as Rec_LDZCommodityCost,
                                                SUM(gas.AmendmentCommodityCharge)  as Rec_WholesaleCost,
                                                count(*)                           as rowcount
                                         FROM vw_fin_gross_margin_journals_gas_msgsp gas
                                       where timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
                    group by gas.gspgroupid_1
                    order by gas.gspgroupid_1) b
                on a.gspgroupid = b.gspgroupid
              order by a.gspgroupid
              )
              c
              inner join
              (
              select gas.gspgroupid_1 as gspgroupid,
              SUM (gas.TheoreticalRevenue) as TheoreticalRevenue,
              SUM (gas.TheoreticalStandingCharge) as TheoreticalStandingCharge,
              SUM (gas.Billed_Revenue) as BilledRevenue,
              SUM (gas.ReprofiledChargeAmount) as ReprofiledChargeAmount,
              SUM (gas.BilledStandingCharge) as BilledStandingCharge,
              SUM (gas.LDZCommodityCost) as LDZCommodityCost,
              SUM (gas.LDZCapacityCost) as LDZCapacityCost,
              SUM (gas.CustomerCapacityCost) as CustomerCapacityCost,
              SUM (gas.CustomerFixedCost) as CustomerFixedCost,
              SUM (gas.NTSCommodityCost) as NTSCommodityCost,
              SUM (gas.LDZExitCapacityCost) as LDZExitCapacityCost,
              SUM (gas.WholesaleCost) as WholesaleCost,
              SUM (gas.BalancingFee) as BalancingFee,
              SUM (gas.ManagementFee) as ManagementFee,
              SUM (gas.reconciliation_revenue) as Rec_TheoreticalRevenue,
              SUM (gas.AmendmentNTSCommodityCost) as Rec_NTSCommodityCost,
              SUM (gas.AmendmentLDZCommodityCost) as Rec_LDZCommodityCost,
              SUM (gas.AmendmentCommodityCharge) as Rec_WholesaleCost,
              count (*) as rowcount
              FROM (select gas.*,
                gsp.gspgroupid,
                msgsp.gsp,
              coalesce (case when (gsp.gspgroupid) = '' then null else gsp.gspgroupid end, msgsp.gsp) as gspgroupid_1
              from aws_fin_stage1_extracts.fin_gross_margin_journals_gas gas
                             left join public.vw_fin_gross_margin_gas_gsp gsp on gas.accountid = gsp.accountid
                             left join aws_fin_stage1_extracts.fin_gross_margin_missing_gsp msgsp
              on gsp.accountid = msgsp.accountid) gas
              where date (consumptionmonth) = '$ReportMonth'---cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
                and timestamp = '$ReportMonth'
              group by gas.gspgroupid_1
              order by gas.gspgroupid_1
              ) d
              on c.gspgroupid = d.gspgroupid



UNION

--- Total ---

 select 'Total' as gspgroupid, stg_Tots.*, 1 as key
 from ( select ---gas.gspgroupid_1                   as gspgroupid,
         SUM(TheoreticalRevenue)        as "TheoreticalRevenue",
         SUM(TheoreticalStandingCharge) as "TheoreticalStandingCharge",
         SUM(BilledRevenue)            as "BilledRevenue",
         SUM(ReprofiledChargeAmount)    as "ReprofiledChargeAmount",
         SUM(BilledStandingCharge)      as "BilledStandingCharge",
         SUM(LDZCommodityCost)          as "LDZCommodityCost",
         SUM(LDZCapacityCost)           as "LDZCapacityCost",
         SUM(CustomerCapacityCost)      as "CustomerCapacityCost",
         SUM(CustomerFixedCost)         as "CustomerFixedCost",
         SUM(NTSCommodityCost)          as "NTSCommodityCost",
         SUM(LDZExitCapacityCost)       as "LDZExitCapacityCost",
         SUM(WholesaleCost)             as "WholesaleCost",
         SUM(BalancingFee)              as "BalancingFee",
         SUM(ManagementFee)             as "ManagementFee",
         SUM(Rec_TheoreticalRevenue)    as "Rec_TheoreticalRevenue",
         SUM(Rec_NTSCommodityCost) as "Rec_NTSCommodityCost",
         SUM(Rec_LDZCommodityCost) as "Rec_LDZCommodityCost",
         SUM(Rec_WholesaleCost)  as "Rec_WholesaleCost"
         from(
                select c.gspgroupid,
                       c.TheoreticalRevenue - d.TheoreticalRevenue               as "TheoreticalRevenue",
                       c.TheoreticalStandingCharge - d.TheoreticalStandingCharge as "TheoreticalStandingCharge",
                       c.BilledRevenue - d.BilledRevenue                         as "BilledRevenue",
                       c.ReprofiledChargeAmount - d.ReprofiledChargeAmount       as "ReprofiledChargeAmount",
                       c.BilledStandingCharge - d.BilledStandingCharge           as "BilledStandingCharge",
                       c.LDZCommodityCost - d.LDZCommodityCost                   as "LDZCommodityCost",
                       c.LDZCapacityCost - d.LDZCapacityCost                     as "LDZCapacityCost",
                       c.CustomerCapacityCost - d.CustomerCapacityCost           as "CustomerCapacityCost",
                       c.CustomerFixedCost - d.CustomerFixedCost                 as "CustomerFixedCost",
                       c.NTSCommodityCost - d.NTSCommodityCost                   as "NTSCommodityCost",
                       c.LDZExitCapacityCost - d.LDZExitCapacityCost             as "LDZExitCapacityCost",
                       c.WholesaleCost - d.WholesaleCost                         as "WholesaleCost",
                       c.BalancingFee - d.BalancingFee                           as "BalancingFee",
                       c.ManagementFee - d.ManagementFee                         as "ManagementFee",
                       c.Rec_TheoreticalRevenue - d.Rec_TheoreticalRevenue       as "Rec_TheoreticalRevenue",
                       c.Rec_NTSCommodityCost - d.Rec_NTSCommodityCost           as "Rec_NTSCommodityCost",
                       c.Rec_LDZCommodityCost - d.Rec_LDZCommodityCost           as "Rec_LDZCommodityCost",
                       c.Rec_WholesaleCost - d.Rec_WholesaleCost                 as "Rec_WholesaleCost"
                from (select a.gspgroupid,
                             a.TheoreticalRevenue - b.TheoreticalRevenue               as "TheoreticalRevenue",
                             a.TheoreticalStandingCharge - b.TheoreticalStandingCharge as "TheoreticalStandingCharge",
                             a.BilledRevenue - b.BilledRevenue                         as "BilledRevenue",
                             a.ReprofiledChargeAmount - b.ReprofiledChargeAmount       as "ReprofiledChargeAmount",
                             a.BilledStandingCharge - b.BilledStandingCharge           as "BilledStandingCharge",
                             a.LDZCommodityCost - b.LDZCommodityCost                   as "LDZCommodityCost",
                             a.LDZCapacityCost - b.LDZCapacityCost                     as "LDZCapacityCost",
                             a.CustomerCapacityCost - b.CustomerCapacityCost           as "CustomerCapacityCost",
                             a.CustomerFixedCost - b.CustomerFixedCost                 as "CustomerFixedCost",
                             a.NTSCommodityCost - b.NTSCommodityCost                   as "NTSCommodityCost",
                             a.LDZExitCapacityCost - b.LDZExitCapacityCost             as "LDZExitCapacityCost",
                             a.WholesaleCost - b.WholesaleCost                         as "WholesaleCost",
                             a.BalancingFee - b.BalancingFee                           as "BalancingFee",
                             a.ManagementFee - b.ManagementFee                         as "ManagementFee",
                             a.Rec_TheoreticalRevenue - b.Rec_TheoreticalRevenue       as "Rec_TheoreticalRevenue",
                             a.Rec_NTSCommodityCost - b.Rec_NTSCommodityCost           as "Rec_NTSCommodityCost",
                             a.Rec_LDZCommodityCost - b.Rec_LDZCommodityCost           as "Rec_LDZCommodityCost",
                             a.Rec_WholesaleCost - b.Rec_WholesaleCost                 as "Rec_WholesaleCost"
                      from (select gas.gspgroupid_1                   as gspgroupid,
                                   SUM(gas.TheoreticalRevenue)        as TheoreticalRevenue,
                                   SUM(gas.TheoreticalStandingCharge) as TheoreticalStandingCharge,
                                   SUM(gas.Billed_Revenue)            as BilledRevenue,
                                   SUM(gas.ReprofiledChargeAmount)    as ReprofiledChargeAmount,
                                   SUM(gas.BilledStandingCharge)      as BilledStandingCharge,
                                   SUM(gas.LDZCommodityCost)          as LDZCommodityCost,
                                   SUM(gas.LDZCapacityCost)           as LDZCapacityCost,
                                   SUM(gas.CustomerCapacityCost)      as CustomerCapacityCost,
                                   SUM(gas.CustomerFixedCost)         as CustomerFixedCost,
                                   SUM(gas.NTSCommodityCost)          as NTSCommodityCost,
                                   SUM(gas.LDZExitCapacityCost)       as LDZExitCapacityCost,
                                   SUM(gas.WholesaleCost)             as WholesaleCost,
                                   SUM(gas.BalancingFee)              as BalancingFee,
                                   SUM(gas.ManagementFee)             as ManagementFee,
                                   SUM(gas.reconciliation_revenue)    as Rec_TheoreticalRevenue,
                                   SUM(gas.AmendmentNTSCommodityCost) as Rec_NTSCommodityCost,
                                   SUM(gas.AmendmentLDZCommodityCost) as Rec_LDZCommodityCost,
                                   SUM(gas.AmendmentCommodityCharge)  as Rec_WholesaleCost,
                                   count(*)                           as rowcount
                            FROM vw_fin_gross_margin_journals_gas_msgsp gas
                            where timestamp = '$ReportMonth'
                            group by gas.gspgroupid_1
                            order by gas.gspgroupid_1) a
                             inner join (select gas.gspgroupid_1                   as gspgroupid,
                                                SUM(gas.TheoreticalRevenue)        as TheoreticalRevenue,
                                                SUM(gas.TheoreticalStandingCharge) as TheoreticalStandingCharge,
                                                SUM(gas.Billed_Revenue)            as BilledRevenue,
                                                SUM(gas.ReprofiledChargeAmount)    as ReprofiledChargeAmount,
                                                SUM(gas.BilledStandingCharge)      as BilledStandingCharge,
                                                SUM(gas.LDZCommodityCost)          as LDZCommodityCost,
                                                SUM(gas.LDZCapacityCost)           as LDZCapacityCost,
                                                SUM(gas.CustomerCapacityCost)      as CustomerCapacityCost,
                                                SUM(gas.CustomerFixedCost)         as CustomerFixedCost,
                                                SUM(gas.NTSCommodityCost)          as NTSCommodityCost,
                                                SUM(gas.LDZExitCapacityCost)       as LDZExitCapacityCost,
                                                SUM(gas.WholesaleCost)             as WholesaleCost,
                                                SUM(gas.BalancingFee)              as BalancingFee,
                                                SUM(gas.ManagementFee)             as ManagementFee,
                                                SUM(gas.reconciliation_revenue)    as Rec_TheoreticalRevenue,
                                                SUM(gas.AmendmentNTSCommodityCost) as Rec_NTSCommodityCost,
                                                SUM(gas.AmendmentLDZCommodityCost) as Rec_LDZCommodityCost,
                                                SUM(gas.AmendmentCommodityCharge)  as Rec_WholesaleCost,
                                                count(*)                           as rowcount
                                         FROM vw_fin_gross_margin_journals_gas_msgsp gas
                                       where timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
                    group by gas.gspgroupid_1
                    order by gas.gspgroupid_1) b
                on a.gspgroupid = b.gspgroupid
              order by a.gspgroupid
              )
              c
              inner join
              (
              select gas.gspgroupid_1 as gspgroupid,
              SUM (gas.TheoreticalRevenue) as TheoreticalRevenue,
              SUM (gas.TheoreticalStandingCharge) as TheoreticalStandingCharge,
              SUM (gas.Billed_Revenue) as BilledRevenue,
              SUM (gas.ReprofiledChargeAmount) as ReprofiledChargeAmount,
              SUM (gas.BilledStandingCharge) as BilledStandingCharge,
              SUM (gas.LDZCommodityCost) as LDZCommodityCost,
              SUM (gas.LDZCapacityCost) as LDZCapacityCost,
              SUM (gas.CustomerCapacityCost) as CustomerCapacityCost,
              SUM (gas.CustomerFixedCost) as CustomerFixedCost,
              SUM (gas.NTSCommodityCost) as NTSCommodityCost,
              SUM (gas.LDZExitCapacityCost) as LDZExitCapacityCost,
              SUM (gas.WholesaleCost) as WholesaleCost,
              SUM (gas.BalancingFee) as BalancingFee,
              SUM (gas.ManagementFee) as ManagementFee,
              SUM (gas.reconciliation_revenue) as Rec_TheoreticalRevenue,
              SUM (gas.AmendmentNTSCommodityCost) as Rec_NTSCommodityCost,
              SUM (gas.AmendmentLDZCommodityCost) as Rec_LDZCommodityCost,
              SUM (gas.AmendmentCommodityCharge) as Rec_WholesaleCost,
              count (*) as rowcount
              FROM (select gas.*,
                gsp.gspgroupid,
                msgsp.gsp,
              coalesce (case when (gsp.gspgroupid) = '' then null else gsp.gspgroupid end, msgsp.gsp) as gspgroupid_1
              from aws_fin_stage1_extracts.fin_gross_margin_journals_gas gas
                             left join public.vw_fin_gross_margin_gas_gsp gsp on gas.accountid = gsp.accountid
                             left join aws_fin_stage1_extracts.fin_gross_margin_missing_gsp msgsp
              on gsp.accountid = msgsp.accountid) gas
              where date (consumptionmonth) = '$ReportMonth'---cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
                and timestamp = '$ReportMonth'
              group by gas.gspgroupid_1
              order by gas.gspgroupid_1
              ) d
              on c.gspgroupid = d.gspgroupid
              order by c.gspgroupid) gas) stg_Tots)tots
order by key, gspgroupid;
