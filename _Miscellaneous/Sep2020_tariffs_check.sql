select tc.account_id,
       tc.fuel,
       error_code,
       last_updated,
       datediff(days, trunc(last_updated), trunc(etlchange)) as days_in_state,
       ssd,
       sed,
       reg_status,
       igl_trf_missing,
       igl_trf_standing_charge,
       igl_trf_unit_rate,
       igl_trf_start_date,
       igl_trf_end_date,
       ens_trf_missing,
       ens_trf_standing_charge,
       ens_trf_unit_rate,
       ens_trf_start_date,
       ens_trf_end_date,
       trf_mismatch,
       etlchange
from ref_calculated_igl_ens_tariff_comparison tc
         left join (select account_id, fuel, max(etlchange) as last_updated
                    from ref_calculated_igl_ens_tariff_comparison_audit
                    group by account_id, fuel) tc_lu on tc.account_id = tc_lu.account_id and tc.fuel = tc_lu.fuel
where right(error_code, 5) != 'Valid'
  and reg_status != 'Final'
--   and days_in_state > 1