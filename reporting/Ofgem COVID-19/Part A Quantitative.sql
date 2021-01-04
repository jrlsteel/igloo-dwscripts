create or replace view vw_ofgem_covid19_v2 as
with unused as (select null),
     cte_date_range as (
         select dateadd(second, -1, date_trunc('month', getdate())) as month_end,
                date_trunc('month', month_end)                      as month_start
     ),
     cte_payments_in_month as (
         select rat.account_id,
                rep_month.month_end as month_ending,
                count(*)            as num_payments,
                sum(rat.amount)     as total_pay_value
         from public.ref_account_transactions rat
                  inner join cte_date_range rep_month
                             on rat.transactiontype != 'INTEREST' and rat.amount < 0 and
                                trunc(rat.creationdetail_createddate::timestamp) between rep_month.month_start and rep_month.month_end
         group by rat.account_id, rep_month.month_end
     ),
     cte_latest_payment_date as (
         select account_id, max(creationdetail_createddate) as transaction_date, month_end
         from public.ref_account_transactions rat_lp
                  inner join cte_date_range dr_lp on rat_lp.creationdetail_createddate <= dr_lp.month_end
         where rat_lp.transactiontype not in ('BILL', 'INTEREST', 'R')
           and rat_lp.amount < 0
         group by account_id, month_end
     ),
     cte_gc_info as (
         select igl_acc_id,
                date_range.month_start,
                date_range.month_end,
                count(sub.id) > 0                                        as has_active_subscriptions,
                max(sub.amount::int) * 1.0 / 100                         as subscription_amount,
                nvl(max(pay.num_failed) = max(pay.num_attempted), false) as dd_fail,
                max(sub_changes.subscription) is not null                as sub_updated_in_window,
                max(man_cancellations.mandate) is not null               as man_canc_in_window
         from public.vw_gocardless_customer_id_mapping all_ids
                  left join cte_date_range date_range on true
                  left join public.vw_mandates_fixed man
                            on all_ids.client_id = man.customerid and
                               man.status in ('active', 'submitted')
                  left join public.vw_subscriptions_fixed sub
                            on sub.mandate = man.mandate_id and sub.status = 'active'
                  left join aws_fin_stage1_extracts.fin_go_cardless_api_events sub_changes
                            on sub_changes.resource_type = 'subscriptions' and
                               sub_changes.action = 'amended' and
                               sub_changes.created_at between date_range.month_start and date_range.month_end and
                               sub_changes.subscription = sub.id
                  left join (select subscription,
                                    mandate,
                                    date_trunc('month', charge_date::timestamp) as month_commencing,
                                    sum((status = 'failed')::int)               as num_failed,
                                    count(*)                                    as num_attempted
                             from public.vw_payments_fixed
                             group by subscription, mandate, month_commencing) pay
                            on pay.mandate = man.mandate_id and
                               month_commencing = date_range.month_start
                  left join public.ref_calculated_daily_customer_file dcf
                            on dcf.account_id = all_ids.igl_acc_id
                  left join public.vw_mandates_fixed canc_man
                            on all_ids.client_id = canc_man.customerid
                  left join aws_fin_stage1_extracts.fin_go_cardless_api_events man_cancellations
                            on man_cancellations.resource_type = 'mandates' and
                               man_cancellations.action = 'cancelled' and
                               man_cancellations.created_at::timestamp between date_range.month_start and date_range.month_end and
                               man_cancellations.mandate = canc_man.mandate_id
         group by all_ids.igl_acc_id, date_range.month_start,
                  date_range.month_end
     ),
     cte_distinct_customer_file as (
         select distinct account_id,
                         account_status,
                         acc_ssd,
                         acc_ed,
                         days_as_occ_acc,
                         eac_igloo_ca,
                         num_elec_mpns,
                         num_gas_mpns,
                         aq_igloo_ca,
                         first_bill_date,
                         latest_bill_date
         from public.ref_calculated_daily_customer_file
     ),
     cte_outstanding_bills as (
         select *, row_number() over (partition by account_id order by bill_date) as ob_number
         from (select bill.account_id,
                      bill.creationdetail_createddate                   as bill_date,
                      bill.currentbalance                               as balance_at_bill,
                      bill.amount                                       as bill_amount,
                      sum(payments.amount)                              as payments_since,
                      greatest(0, bill.currentbalance + payments_since) as outstanding_charge,
                      outstanding_charge::int = 0                       as paid_off,
                      date_range.month_end                              as calc_month_end
               from public.ref_account_transactions bill
                        inner join cte_date_range date_range
                                   on bill.creationdetail_createddate <= date_range.month_end
                        left join public.ref_account_transactions payments
                                  on payments.creationdetail_createddate between bill.creationdetail_createddate and date_range.month_end and
                                     payments.account_id = bill.account_id and
                                     payments.amount < 0
               group by bill.account_id, bill_amount, bill_date, balance_at_bill, month_end) bills
         where not paid_off
     ),
     cte_accounts_in_arrears as (
         select first_ob.account_id,
                first_ob.bill_date                                            as first_unpaid_bill_date,
                first_ob.outstanding_charge + sum(subsequent_obs.bill_amount) as arrears_remaining,
                date_range.month_end
         from cte_outstanding_bills first_ob
                  inner join cte_date_range date_range
                             on datediff(days, first_ob.bill_date, date_range.month_end) > 91 and
                                first_ob.calc_month_end = date_range.month_end
                  left join cte_outstanding_bills subsequent_obs
                            on first_ob.account_id = subsequent_obs.account_id and subsequent_obs.ob_number > 1
         where first_ob.ob_number = 1
         group by first_ob.account_id, first_ob.bill_date, first_ob.outstanding_charge,
                  date_range.month_end
     ),
     cte_greatest_balance as (
         select account_id, min(currentbalance) as best_balance, date_range.month_end
         from public.ref_account_transactions balances
                  inner join cte_date_range date_range
                             on balances.creationdetail_createddate between date_range.month_start and date_range.month_end
         group by account_id, month_end
     ),
     cte_account_figures as (
         select distinct dcf.account_id,
                         up.user_id,
                         dcf.account_status != 'Cancelled' and
                         dcf.acc_ssd <= date_range.month_end and
                         nvl(acc_ed, getdate() + 1000) >= date_range.month_end                        as acc_live,
                         dcf.days_as_occ_acc is not null and
                         date_range.month_end between dcf.acc_ssd and (dcf.acc_ssd + days_as_occ_acc) as occ_acc_month_end,
                         attr_psr.attribute_custom_value is not null                                  as psr,
                         ((num_elec_mpns * vlr.elec_sc * 365) + (eac_igloo_ca * vlr.elec_ur)) * 0.01 *
                         1.05 / -- VLR needs to be updated to something that accounts for timespan
                         12                                                                           as elec_monthly_usage_sterling,
                         ((num_gas_mpns * vlr.gas_sc * 365) + (aq_igloo_ca * vlr.gas_ur)) * 0.01 * 1.05 /
                         12                                                                           as gas_monthly_usage_sterling,
                         nvl(elec_monthly_usage_sterling, 0) +
                         nvl(gas_monthly_usage_sterling, 0)                                           as monthly_usage_sterling,
                         bal.best_balance,
                         monthly_usage_sterling + (nvl(bal.best_balance, 0.0) / 12)                   as proposed_dd,
                         nvl(gc_info.has_active_subscriptions::int, 0)                                as has_active_subscription,
                         nvl(gc_info.subscription_amount, 0)                                          as current_dd,
                         nvl(gc_info.dd_fail, false)                                                  as dd_failure,
                         nvl(gc_info.man_canc_in_window and (has_active_subscription = 0),
                             false)                                                                   as man_canc_in_window,
                         nvl(gc_info.sub_updated_in_window, false)                                    as dd_amended,
                         date_range.month_start,
                         date_range.month_end,
                         dcf.first_bill_date,
                         dcf.latest_bill_date,
                         nvl(cte_pim.num_payments, 0) > 0                                             as pay_in_month,
                         nvl(cte_pim.total_pay_value, 0)                                              as total_monthly_payment,
                         pay_lay_wu.amount is not null                                                as wu_removed,
                         pl_pay_plan.amount is not null                                               as on_repayment_plan,
                         pl_pay_plan.amount                                                           as repayment_plan_amount,
                         arr.account_id is not null                                                   as in_arrears,
                         arr.arrears_remaining                                                        as arrears_gbp,
                         in_arrears and not on_repayment_plan                                         as unmanaged_arrears,
                         dd_amended and current_dd < monthly_usage_sterling                           as active_financial_relief,
                         case
                             when active_financial_relief then monthly_usage_sterling - current_dd
                             else 0 end                                                               as fr_in_month,
                         fr_in_month                                                                  as fr_total,
                         datediff(days, lpd.transaction_date, date_range.month_end)                   as days_since_payment,
                         case
                             when days_since_payment is null then fr_total * 0.75
                             when days_since_payment > 60 then fr_total * 0.47
                             else 0 end                                                               as expected_unrecoverable_fr_gbp
         from cte_distinct_customer_file dcf
                  cross join cte_date_range date_range
                  left join cte_payments_in_month cte_pim
                            on cte_pim.account_id = dcf.account_id
                                and cte_pim.month_ending = trunc(date_range.month_end)
                  left join public.ref_cdb_supply_contracts sc
                            on dcf.account_id = sc.external_id
                  left join public.ref_cdb_user_permissions up
                            on up.permissionable_type = 'App\\SupplyContract' and
                               up.permissionable_id = sc.id and
                               up.permission_level = 0
                  left join public.ref_cdb_attributes attr_psr
                            on attr_psr.attribute_type_id = 17 and
                               attr_psr.attribute_custom_value != '[]' and
                               attr_psr.effective_from <= date_range.month_end and
                               nvl(attr_psr.effective_to, getdate() + 1000) >
                               date_range.month_end and
                               attr_psr.entity_id = sc.id
                  left join public.vw_latest_rates_ensek vlr
                            on vlr.account_id = dcf.account_id
                  left join cte_gc_info gc_info
                            on gc_info.igl_acc_id = dcf.account_id and
                               date_range.month_end = gc_info.month_end
                  left join aws_s3_stage2_extracts.stage2_cdbpaymentlayers pay_lay_wu
                            on pay_lay_wu.supply_contract_id = sc.id and
                               pay_lay_wu.payment_type_id = 5 and
                               pay_lay_wu.effective_to between date_range.month_start and date_range.month_end
                  left join aws_s3_stage2_extracts.stage2_cdbpaymentlayers pl_pay_plan
                            on pl_pay_plan.payment_type_id = 4 and
                               pl_pay_plan.supply_contract_id = sc.id and
                               date_range.month_end between pl_pay_plan.effective_from and nvl(pl_pay_plan.effective_to, getdate() + 1)
                  left join cte_accounts_in_arrears arr
                            on dcf.account_id = arr.account_id and arr.month_end = date_range.month_end
                  left join cte_greatest_balance bal
                            on bal.account_id = dcf.account_id and bal.month_end = date_range.month_end
                  left join cte_latest_payment_date lpd
                            on lpd.account_id = dcf.account_id and date_range.month_end = lpd.month_end
     ),
     cte_live_account_summaries as (
         select month_start                                                      as month_commencing,
                sum(acc_live::int)                                               as num_on_supply,
                sum(has_active_subscription::int)                                as total_dd_instructions,
                sum(occ_acc_month_end::int)                                      as occ_accs_month_end,
                sum((man_canc_in_window
                    and psr)::int)                                               as psr_dd_cancellations,
                sum((man_canc_in_window)::int)                                   as dd_cancellations,
                sum(((current_dd < proposed_dd)
                    and dd_amended
                    and psr)::int)                                               as psr_low_dd,
                sum(((current_dd < proposed_dd)
                    and dd_amended)::int)                                        as low_dd,
                sum(((current_dd < proposed_dd)
                    and dd_amended
                    and psr
                    and not wu_removed)::int)                                    as psr_pay_hol,
                sum(((current_dd < proposed_dd)
                    and dd_amended
                    and not wu_removed)::int)                                    as pay_hol,
                sum(case
                        when ((current_dd < proposed_dd)
                            and dd_amended
                            and psr
                            and not wu_removed)
                            then (proposed_dd - current_dd)
                        else 0 end)                                              as psr_pay_hol_pounds,
                sum(case
                        when ((current_dd < proposed_dd)
                            and dd_amended
                            and not wu_removed)
                            then (proposed_dd - current_dd)
                        else 0 end)                                              as pay_hol_pounds,
                sum((dd_failure
                    and psr)::int)                                               as psr_dd_fail,
                sum(dd_failure::int)                                             as dd_fail,
                round(sum(((current_dd >= proposed_dd)
                    and psr)::int)::double precision * 100 /
                      nullif(sum(psr::int), 0)::double precision)                as psr_correct_dd,
                sum((current_dd >= proposed_dd)::int)                            as correct_dd,
                sum((occ_acc_month_end
                    and first_bill_date <= month_start
                    and best_balance > 0)::int)                                  as occ_acc_pay_fail,
                occ_accs_month_end - occ_acc_pay_fail                            as occ_acc_pay_success,
                sum(psr::int)                                                    as num_psr,
                sum(on_repayment_plan::int)                                      as num_rep_plans,
                sum(unmanaged_arrears::int)                                      as num_unmanaged_arrears,
                sum(arrears_gbp) / count(arrears_gbp)                            as average_arrears_gbp,
                sum(active_financial_relief::int)                                as ongoing_fin_relief,
                sum(fr_in_month)                                                 as fin_relief_this_month,
                sum(expected_unrecoverable_fr_gbp)                               as unrecoverable_fin_relief,
                sum((active_financial_relief and psr)::int)                      as ongoing_fin_relief_psr,
                sum(case when psr then fr_in_month else 0 end)                   as fin_relief_this_month_psr,
                sum(case when psr then expected_unrecoverable_fr_gbp else 0 end) as unrecoverable_fin_relief_psr
         from cte_account_figures account_figures
         where acc_live
         group by month_start
         order by month_commencing desc
     ),
     cte_final_account_summaries as (
         select month_start                  as month_commencing,
                sum(on_repayment_plan::int)  as num_rep_plans,
                sum((best_balance > 0)::int) as num_final_pay_fail
         from cte_account_figures account_figures
         where not acc_live
         group by month_start
         order by month_commencing desc
     )
select live_account_figures.*,
       final_account_figures.num_rep_plans as rep_plans_final_accs,
       final_account_figures.num_final_pay_fail
from cte_live_account_summaries live_account_figures
         full join cte_final_account_summaries final_account_figures
                   on live_account_figures.month_commencing = final_account_figures.month_commencing
with no schema binding
;


drop table temp_covid19_v2;
create table temp_covid19_v2 as
select *
from vw_ofgem_covid19_v2;

alter table temp_covid19_v2
    owner to igloo;

alter table vw_ofgem_covid19_v2
    owner to igloo;

select *
from vw_ofgem_covid19_v2;

-- -- old version
-- create or replace view public.vw_gocardless_customer_id_mapping as
-- select distinct gc_users.client_id,
--                 nvl(gc_users.ensekid, idl2.accountid, idl.accountid, sc.external_id) as igl_acc_id
-- from aws_fin_stage1_extracts.fin_go_cardless_api_clients gc_users
--          left join public.ref_cdb_users igl_users
--                    on replace(gc_users.email, ' ', '') = replace(igl_users.email, ' ', '')
--          left join public.ref_cdb_user_permissions up
--                    on up.permissionable_type = 'App\\SupplyContract' and permission_level = 0 and user_id = igl_users.id
--          left join public.ref_cdb_supply_contracts sc on up.permissionable_id = sc.id
--          left join aws_fin_stage1_extracts.fin_go_cardless_id_lookup idl on idl.customerid = gc_users.client_id
--          left join aws_fin_stage1_extracts.fin_go_cardless_id_mandate_lookup idl2
--                    on idl2.customerid = gc_users.client_id
-- where igl_acc_id is not null
-- with no schema binding;

-- 2020-12-17
create or replace view public.vw_gocardless_customer_id_mapping as
select combined_mapping.gc_id as client_id, combined_mapping.ensek_id as igl_acc_id
from (select customerid as gc_id, accountid as ensek_id
      from aws_fin_stage1_extracts.fin_go_cardless_id_mandate_lookup
      union
      distinct
      select client_id as gc_id, nullif(ensekid, '')::bigint as ensek_id
      from aws_fin_stage1_extracts.fin_stage2_gocardlessclients) combined_mapping
         left join public.ref_calculated_daily_customer_file dcf on dcf.account_id = combined_mapping.ensek_id
where account_status != 'Cancelled'
  and ensek_id is not null
with no schema binding;


create or replace view vw_subscriptions_fixed as
select sub.id,
       sub.created_at,
       latest_update.event_date              as updated_at,
       amount,
       currency,
       nvl(latest_update.status, sub.status) as status,
       mandate,
       charge_date,
       amount_subscription
from public.ref_fin_gocardless_subscriptions sub
         left join (select *
                    from (select subscription                                                     as sub_id,
                                 action,
                                 case
                                     when action in ('finished', 'cancelled') then 'inactive'
                                     else 'active' end                                            as status,
                                 created_at                                                       as event_date,
                                 row_number() over (partition by sub_id order by created_at desc) as rn
                          from aws_fin_stage1_extracts.fin_go_cardless_api_events
                          where resource_type = 'subscriptions') ordered_sub_events
                    where rn = 1) latest_update on latest_update.sub_id = sub.id
with no schema binding;

create or replace view vw_mandates_fixed as
select man.mandate_id,
       man.customerid,
       man.created_at,
       latest_update.event_date              as updated_at,
       nvl(latest_update.status, man.status) as status
from public.ref_fin_gocardless_mandates man
         left join (select *
                    from (select mandate                                                          as man_id,
                                 action,
                                 case
                                     when action in ('cancelled', 'transferred', 'failed', 'expired') then 'inactive'
                                     else 'active' end                                            as status,
                                 created_at                                                       as event_date,
                                 row_number() over (partition by man_id order by created_at desc) as rn
                          from aws_fin_stage1_extracts.fin_go_cardless_api_events
                          where resource_type = 'mandates') ordered_man_events
                    where rn = 1) latest_update on latest_update.man_id = man.mandate_id
with no schema binding;

-- drop view vw_payments_fixed;
create or replace view vw_payments_fixed as
select pay.id,
       pay.subscription,
       pay.mandate,
       pay.created_at,
       pay.charge_date,
       latest_update.event_date              as updated_at,
       nvl(latest_update.status, pay.status) as status
from public.ref_fin_gocardless_payments pay
         left join (select *
                    from (select payment                                                           as pay_id,
                                 action,
                                 case
                                     when action in ('paid_out', 'confirmed') then 'confirmed'
                                     when action in ('failed', 'resubmission_requested', 'cancelled') then 'failed'
                                     else 'other' end                                              as status,
                                 created_at                                                        as event_date,
                                 row_number() over (partition by payment order by created_at desc) as rn
                          from aws_fin_stage1_extracts.fin_go_cardless_api_events
                          where resource_type = 'payments') ordered_pay_events
                    where rn = 1) latest_update on latest_update.pay_id = pay.id
with no schema binding;



alter table vw_gocardless_customer_id_mapping
    owner to igloo