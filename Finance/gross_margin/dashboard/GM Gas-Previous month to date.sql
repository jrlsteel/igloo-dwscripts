select
    gspgroupid,
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
		Rec_WholesaleCost,
		rowcount
 from (
        select gas.gspgroupid_1                   as gspgroupid,
                   SUM(gas.TheoreticalRevenue)        as "TheoreticalRevenue",
                   SUM(gas.TheoreticalStandingCharge) as "TheoreticalStandingCharge",
                   SUM(gas.Billed_Revenue)            as "BilledRevenue",
                   SUM(gas.ReprofiledChargeAmount)    as "ReprofiledChargeAmount",
                   SUM(gas.BilledStandingCharge)      as "BilledStandingCharge",
                   SUM(gas.LDZCommodityCost)          as "LDZCommodityCost",
                   SUM(gas.LDZCapacityCost)           as "LDZCapacityCost",
                   SUM(gas.CustomerCapacityCost)      as "CustomerCapacityCost",
                   SUM(gas.CustomerFixedCost)         as "CustomerFixedCost",
                   SUM(gas.NTSCommodityCost)          as "NTSCommodityCost",
                   SUM(gas.LDZExitCapacityCost)       as "LDZExitCapacityCost",
                   SUM(gas.WholesaleCost)             as "WholesaleCost",
                   SUM(gas.BalancingFee)              as "BalancingFee",
                   SUM(gas.ManagementFee)             as "ManagementFee",
                   SUM(gas.reconciliation_revenue)    as "Rec_TheoreticalRevenue",
                   SUM(gas.AmendmentNTSCommodityCost) as "Rec_NTSCommodityCost",
                   SUM(gas.AmendmentLDZCommodityCost) as "Rec_LDZCommodityCost",
                   SUM(gas.AmendmentCommodityCharge)  as "Rec_WholesaleCost",
                   count(*)                           as "rowcount",
                   0                                  as  key
            FROM vw_fin_gross_margin_journals_gas_msgsp gas
            WHERE timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
            group by gas.gspgroupid_1

UNION

--- Total ---

 select 'Total' as gspgroupid, stg_Tots.*, 1 as key
          from ( select
                         SUM(gas.TheoreticalRevenue)        as "TheoreticalRevenue",
                         SUM(gas.TheoreticalStandingCharge) as "TheoreticalStandingCharge",
                         SUM(gas.Billed_Revenue)            as "BilledRevenue",
                         SUM(gas.ReprofiledChargeAmount)    as "ReprofiledChargeAmount",
                         SUM(gas.BilledStandingCharge)      as "BilledStandingCharge",
                         SUM(gas.LDZCommodityCost)          as "LDZCommodityCost",
                         SUM(gas.LDZCapacityCost)           as "LDZCapacityCost",
                         SUM(gas.CustomerCapacityCost)      as "CustomerCapacityCost",
                         SUM(gas.CustomerFixedCost)         as "CustomerFixedCost",
                         SUM(gas.NTSCommodityCost)          as "NTSCommodityCost",
                         SUM(gas.LDZExitCapacityCost)       as "LDZExitCapacityCost",
                         SUM(gas.WholesaleCost)             as "WholesaleCost",
                         SUM(gas.BalancingFee)              as "BalancingFee",
                         SUM(gas.ManagementFee)             as "ManagementFee",
                         SUM(gas.reconciliation_revenue)    as "Rec_TheoreticalRevenue",
                         SUM(gas.AmendmentNTSCommodityCost) as "Rec_NTSCommodityCost",
                         SUM(gas.AmendmentLDZCommodityCost) as "Rec_LDZCommodityCost",
                         SUM(gas.AmendmentCommodityCharge)  as "Rec_WholesaleCost",
                         count(*)                           as "rowcount"
                  FROM vw_fin_gross_margin_journals_gas_msgsp gas
                  WHERE timestamp = cast (date_trunc('month', dateadd(month, -1, '$ReportMonth')) as date)
           ) stg_Tots)tots
order by key, gspgroupid;