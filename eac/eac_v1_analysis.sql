/*** EAC_v1 analysis ***/

select
-- t1.account_id,
-- t1.register_id,
t1.category as category_01052019,
t1.reason as reason_01052019,
t2.category as category_02052019,
t2.reason as reason_02052019,
case when (t1.category != t2.category or t1.reason != t2.reason) then
    'Y' else 'N' end as status_changed,
count(*)
 from (
select  t.*,

            case when (igloo_eac_v1 - latest_ind_eac_estimates = 0)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Exact match' else
                  case when (igloo_eac_v1 - latest_ind_eac_estimates != 0 and igloo_eac_v1 - latest_ind_eac_estimates is not null)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Not Exact match' else
                            'Igloo EAC Not calculated' end end  as category,

            case when (igloo_eac_v1 - latest_ind_eac_estimates = 0)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Exact match' else
            case when (igloo_eac_v1 = 0 or igloo_eac_v1 is null) then
                case when (read_days_diff_elec = 0 or read_days_diff_elec is null) then
                        'Not Enough reads for calculation' else
                    case when (read_consumption_elec = 0 or read_consumption_elec is null) then
                        'Consumption is zero' else
                        case when (previous_ind_eac_estimates = 0 or previous_ind_eac_estimates is null) then
                            'Previous_EAC is not available for calculation' else
                            case when ((ppc is null or ppc = 0) and (bpp =0 or bpp is null)) then
                                'No PPC or BPP is available from d18 for calculation' end end end end else

            case when (igloo_eac_v1 - latest_ind_eac_estimates != 0 and igloo_eac_v1 - latest_ind_eac_estimates is not null) and (igloo_eac_v1 != 0 or igloo_eac_v1 is not null) then
                case when (latest_ind_eac_estimates = 0 or latest_ind_eac_estimates is null) then
                    'Latest EAC from Industry not available yet' else
                    case when (ppc is not null and no_of_ppc_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec)) or
                            (bpp is not null and no_of_bpp_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec)) then
                            'Only Partial ppc or bpp rows available' else
                        case when ((igloo_eac_v1 - latest_ind_eac_estimates between 1 and 10) or (igloo_eac_v1 - latest_ind_eac_estimates between -10 and -1)) then
                              'unknown(Within 10 units)' else
                            case when (igloo_eac_v1 - latest_ind_eac_estimates between 10 and 50 or igloo_eac_v1 - latest_ind_eac_estimates between -50 and -10) then
                                'unknown(Within 50 units)' else
                                      'unknown'
                                        end end end end end end end
                                            as reason,

       case when ppc is null or no_of_ppc_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec) then
          1 else 0
          end as bpp_used

from ref_calculated_eac_v1_audit t
) t1
left outer join (
select
       t.*,

            case when (igloo_eac_v1 - latest_ind_eac_estimates = 0)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Exact match' else
                  case when (igloo_eac_v1 - latest_ind_eac_estimates != 0 and igloo_eac_v1 - latest_ind_eac_estimates is not null)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Not Exact match' else
                            'Igloo EAC Not calculated' end end  as category,

            case when (igloo_eac_v1 - latest_ind_eac_estimates = 0)
                      and (igloo_eac_v1 != 0 and igloo_eac_v1 is not null) then
                          'Exact match' else
            case when (igloo_eac_v1 = 0 or igloo_eac_v1 is null) then
                case when (read_days_diff_elec = 0 or read_days_diff_elec is null) then
                        'Not Enough reads for calculation' else
                    case when (read_consumption_elec = 0 or read_consumption_elec is null) then
                        'Consumption is zero' else
                        case when (previous_ind_eac_estimates = 0 or previous_ind_eac_estimates is null) then
                            'Previous_EAC is not available for calculation' else
                            case when ((ppc is null or ppc = 0) and (bpp =0 or bpp is null)) then
                                'No PPC or BPP is available from d18 for calculation' end end end end else

            case when (igloo_eac_v1 - latest_ind_eac_estimates != 0 and igloo_eac_v1 - latest_ind_eac_estimates is not null) and (igloo_eac_v1 != 0 or igloo_eac_v1 is not null) then
                case when (latest_ind_eac_estimates = 0 or latest_ind_eac_estimates is null) then
                    'Latest EAC from Industry not available yet' else
                    case when (ppc is not null and no_of_ppc_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec)) or
                            (bpp is not null and no_of_bpp_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec)) then
                            'Only Partial ppc or bpp rows available' else
                        case when ((igloo_eac_v1 - latest_ind_eac_estimates between 1 and 10) or (igloo_eac_v1 - latest_ind_eac_estimates between -10 and -1)) then
                              'unknown(Within 10 units)' else
                            case when (igloo_eac_v1 - latest_ind_eac_estimates between 10 and 50 or igloo_eac_v1 - latest_ind_eac_estimates between -50 and -10) then
                                'unknown(Within 50 units)' else
                                      'unknown'
                                        end end end end end end end
                                            as reason,

       case when ppc is null or no_of_ppc_rows < datediff(days, read_min_datetime_elec, read_max_datetime_elec) then
          1 else 0
          end as bpp_used

from ref_calculated_eac_v1_audit t where trunc(etlchange) = ${etlchange2}
) t2
on t1.account_id = t2.account_id and t1.meterpoint_id = t2.meterpoint_id
            and t1.register_id = t2.register_id and trunc(t2.etlchange) = ${etlchange2}
where
trunc(t1.etlchange) = ${etlchange1}
-- and t1.category = 'Exact match' and t2.category in('Igloo EAC Not calculated')
group by
-- t1.account_id,
-- t1.register_id,
t1.category,
t1.reason,
t2.category,
t2.reason
order by t1.category;
