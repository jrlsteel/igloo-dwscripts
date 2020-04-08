with account_figures as (
    with date_range as (select '2020-03-30 00:00:00'::timestamp as monday,
                               '2020-04-05 23:59:59'::timestamp as sunday)
    select dcf.account_id,
           up.user_id,
           dcf.account_status != 'Cancelled' and
           dcf.acc_ssd <= date_range.sunday and
           nvl(acc_ed, getdate()) >= date_range.sunday                                                 as acc_live,
           nvl(dcf.latest_bill_date, getdate() + 1000) between date_range.monday and date_range.sunday as bill_in_window,
           attr_psr.attribute_custom_value is not null                                                 as psr,
           ((num_elec_mpns * vlr.elec_sc * 365) + (eac_igloo_ca * vlr.elec_ur)) * 0.01 * 1.05 /
           12                                                                                          as elec_monthly_usage_sterling,
           ((num_gas_mpns * vlr.gas_sc * 365) + (aq_igloo_ca * vlr.gas_ur)) * 0.01 * 1.05 /
           12                                                                                          as gas_monthly_usage_sterling,
           nvl(elec_monthly_usage_sterling, 0) +
           nvl(gas_monthly_usage_sterling, 0)                                                          as monthly_usage_sterling,
           nvl(cp.num_declined_payments, 0)                                                            as num_declined_payments,
           nvl(gc_info.has_active_subscriptions::int, 0)                                               as has_active_subscription,
           nvl(gc_info.subscription_amount, 0)                                                         as current_dd,
           nvl(gc_info.dd_fail, false)                                                                 as dd_failure,
           nvl(man_canc_in_window and (has_active_subscription = 0), false)                            as man_canc_in_window,
           nvl(sub_updated_in_window, false)                                                           as dd_amended
    from ref_calculated_daily_customer_file dcf
             left join date_range on true
             left join ref_cdb_supply_contracts sc on dcf.account_id = sc.external_id
             left join ref_cdb_user_permissions up
                       on up.permissionable_type = 'App\\SupplyContract' and
                          up.permissionable_id = sc.id and
                          up.permission_level = 0
             left join ref_cdb_attributes attr_psr on attr_psr.attribute_type_id = 17 and
                                                      attr_psr.attribute_custom_value != '[]' and
                                                      attr_psr.effective_to is null and
                                                      attr_psr.entity_id = sc.id
             left join vw_latest_rates_ensek vlr on vlr.account_id = dcf.account_id
             left join (select reference_id                              as supply_contract_id,
                               date_trunc('week', created_at::timestamp) as week_beginning,
                               count(*)                                  as num_declined_payments
                        from aws_s3_stage2_extracts.stage2_cdbcustomerpayments
                        where status = 'declined'
                          and reference_type = 'App\\SupplyContract'
                        group by supply_contract_id, week_beginning) cp
                       on cp.supply_contract_id = sc.id and
                          cp.week_beginning = date_range.monday
             left join (select igl_acc_id,
                               count(sub.id) > 0                                        as has_active_subscriptions,
                               max(sub.amount::int)                                     as subscription_amount,
                               nvl(max(pay.num_failed) = max(pay.num_attempted), false) as dd_fail,
                               max(sub_changes.subscription) is not null                as sub_updated_in_window,
                               max(man_cancellations.mandate) is not null               as man_canc_in_window
                        from vw_gocardless_customer_id_mapping all_ids
                                 left join date_range on true
                                 left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
                                           on all_ids.client_id = man.customerid and
                                              man.status in ('active', 'submitted')
                                 left join aws_fin_stage1_extracts.fin_go_cardless_api_subscriptions sub
                                           on sub.mandate = man.mandate_id and sub.status = 'active'
                                 left join aws_fin_stage1_extracts.fin_go_cardless_api_events sub_changes
                                           on sub_changes.resource_type = 'subscriptions' and
                                              sub_changes.action = 'amended' and
                                              sub_changes.created_at between date_range.monday and date_range.sunday and
                                              sub_changes.subscription = sub.id
                                 left join (select subscription,
                                                   mandate,
                                                   date_trunc('week', charge_date::timestamp) as week_commencing,
                                                   sum((status = 'failed')::int)              as num_failed,
                                                   count(*)                                   as num_attempted
                                            from aws_fin_stage1_extracts.fin_go_cardless_api_payments
                                            group by subscription, mandate, week_commencing) pay
                                           on pay.mandate = man.mandate_id and
                                              week_commencing = date_range.monday
                                 left join ref_calculated_daily_customer_file dcf
                                           on dcf.account_id = all_ids.igl_acc_id
                                 left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates canc_man
                                           on all_ids.client_id = canc_man.customerid and
                                              canc_man.status = 'cancelled'
                                 left join aws_fin_stage1_extracts.fin_go_cardless_api_events man_cancellations
                                           on man_cancellations.resource_type = 'mandates' and
                                              man_cancellations.action = 'cancelled' and
                                              man_cancellations.created_at between date_range.monday and date_range.sunday and
                                              man_cancellations.mandate = canc_man.mandate_id
                        group by all_ids.igl_acc_id) gc_info
                       on gc_info.igl_acc_id = dcf.account_id)
select sum(acc_live::int)                                                       as num_on_supply,
       sum(has_active_subscription::int)                                        as total_dd_instructions,
       sum(bill_in_window::int)                                                 as total_billed_in_window,
       sum((man_canc_in_window and psr)::int)                                   as psr_dd_cancellations,
       sum((man_canc_in_window)::int)                                           as dd_cancellations,
       sum(((current_dd < monthly_usage_sterling) and dd_amended and psr)::int) as psr_low_dd,
       sum(((current_dd < monthly_usage_sterling) and dd_amended)::int)         as low_dd,
       sum((dd_failure and psr)::int)                                           as psr_dd_fail,
       sum(dd_failure::int)                                                     as dd_fail,
       sum(((num_declined_payments > 0) and psr)::int)                          as psr_one_off_pay_fail,
       sum((num_declined_payments > 0)::int)                                    as one_off_pay_fail,
       sum(((current_dd >= monthly_usage_sterling) and psr)::int)::double precision /
       sum(psr::int)::double precision                                          as psr_correct_dd,
       sum((current_dd >= monthly_usage_sterling)::int)::double precision /
       num_on_supply::double precision                                          as correct_dd
from account_figures
where acc_live;



create or replace view vw_gocardless_customer_id_mapping as
select distinct gc_users.client_id,
                nvl(gc_users.ensekid, idl2.accountid, idl.accountid, sc.external_id) as igl_acc_id
from aws_fin_stage1_extracts.fin_go_cardless_api_clients gc_users
         left join public.ref_cdb_users igl_users
                   on replace(gc_users.email, ' ', '') = replace(igl_users.email, ' ', '')
         left join public.ref_cdb_user_permissions up
                   on up.permissionable_type = 'App\\SupplyContract' and permission_level = 0 and user_id = igl_users.id
         left join public.ref_cdb_supply_contracts sc on up.permissionable_id = sc.id
         left join aws_fin_stage1_extracts.fin_go_cardless_id_lookup idl on idl.customerid = gc_users.client_id
         left join aws_fin_stage1_extracts.fin_go_cardless_id_mandate_lookup idl2
                   on idl2.customerid = gc_users.client_id
where igl_acc_id is not null
    with no schema binding

with date_range as (select '2020-01-30 00:00:00'::timestamp as monday,
                           '2020-02-05 23:59:59'::timestamp as sunday)
select igl_acc_id,
       count(sub.id) > 0                                        as has_active_subscriptions,
       max(sub.amount::int)                                     as subscription_amount,
       nvl(max(pay.num_failed) = max(pay.num_attempted), false) as dd_fail,
       max(sub_changes.subscription) is not null                as sub_updated_in_window,
       max(man_cancellations.mandate) is not null               as man_canc_in_window
from vw_gocardless_customer_id_mapping all_ids
         left join date_range on true
         left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
                   on all_ids.client_id = man.customerid and
                      man.status in ('active', 'submitted')
         left join aws_fin_stage1_extracts.fin_go_cardless_api_subscriptions sub
                   on sub.mandate = man.mandate_id and sub.status = 'active'
         left join aws_fin_stage1_extracts.fin_go_cardless_api_events sub_changes
                   on sub_changes.resource_type = 'subscriptions' and
                      sub_changes.action = 'amended' and
                      sub_changes.created_at between date_range.monday and date_range.sunday and
                      sub_changes.subscription = sub.id
         left join (select subscription,
                           mandate,
                           date_trunc('week', charge_date::timestamp) as week_commencing,
                           sum((status = 'failed')::int)              as num_failed,
                           count(*)                                   as num_attempted
                    from aws_fin_stage1_extracts.fin_go_cardless_api_payments
                    group by subscription, mandate, week_commencing) pay
                   on pay.mandate = man.mandate_id and
                      week_commencing = date_range.monday
         left join ref_calculated_daily_customer_file dcf
                   on dcf.account_id = all_ids.igl_acc_id
         left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates canc_man
                   on all_ids.client_id = canc_man.customerid and
                      canc_man.status = 'cancelled'
         left join aws_fin_stage1_extracts.fin_go_cardless_api_events man_cancellations
                   on man_cancellations.resource_type = 'mandates' and
                      man_cancellations.action = 'cancelled' and
                      man_cancellations.created_at between date_range.monday and date_range.sunday and
                      man_cancellations.mandate = canc_man.mandate_id
group by all_ids.igl_acc_id
-- having man_canc_in_window


select *
from vw_gocardless_customer_id_mapping
where igl_acc_id in (1858, 1859, 1860)


select igl_acc_id,
       count(sub.id)            as has_active_subscriptions,
       listagg(sub.amount, ',') as subscription_amount,
       max(pay.num_failed)      as num_pay_fails
from vw_gocardless_customer_id_mapping all_ids
         left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates man
                   on all_ids.client_id = man.customerid and man.status in ('active', 'submitted')
         left join aws_fin_stage1_extracts.fin_go_cardless_api_subscriptions sub
                   on sub.mandate = man.mandate_id and sub.status = 'active'
         left join (select subscription,
                           mandate,
                           date_trunc('week', charge_date::timestamp) as week_commencing,
                           sum((status = 'failed')::int)              as num_failed,
                           count(*)                                   as num_attempted
                    from aws_fin_stage1_extracts.fin_go_cardless_api_payments
                    group by subscription, mandate, week_commencing) pay on pay.mandate = man.mandate_id and
                                                                            week_commencing = '2020-03-30 00:00:00'
         left join ref_calculated_daily_customer_file dcf on dcf.account_id = all_ids.igl_acc_id
group by all_ids.igl_acc_id
order by igl_acc_id

create table temp_gc_id_lookup as
select *
from aws_fin_stage1_extracts.fin_go_cardless_id_lookup

--My details:
-- igl_id = 54977
-- gc_id = CU000JRDYVA5DC
-- man_id = MD000JJZCJM91M
-- sub_id = (not in database) SB0006PN8JEGZB

select *
from vw_gocardless_customer_id_mapping
where igl_acc_id = 1857

select *
from vw_gocardless_customer_id_mapping
where igl_acc_id = 54977


select *
from aws_fin_stage1_extracts.fin_go_cardless_api_mandates
where customerid in (
                     'CU0006NFNZYQAE',
                     'CU0006K7NGTGC6',
                     'CU0006K4C48PM3',
                     'CU0007HDTBTW2C',
                     'CU00079FZPRVMQ',
                     'CU00079FYTNJCH',
                     'CU00079FHBPX4A',
                     'CU000763GDBGQG',
                     'CU00073NM7B5AP',
                     'CU00070319HBVJ',
                     'CU000702YBZW5M',
                     'CU0006YN9YVAJZ',
                     'CU0006XWZZWR6X',
                     'CU0006XS5NRMKG',
                     'CU0006XJZ75KAY',
                     'CU0006W7173HXM',
                     'CU0006W6ZTH2ZA',
                     'CU0006W6F6CA2Z',
                     'CU0006T5KNH2YK'
    )
  and status = 'active'

select distinct status
from aws_fin_stage1_extracts.fin_go_cardless_api_mandates


select top 200 *
from aws_fin_stage1_extracts.fin_go_cardless_api_mandates
where status = 'cancelled'
order by created_at desc


select distinct(action)
from (SELECT id,
             created_at,
             resource_type,
             action,
             customer_notifications,
             cause,
             description,
             origin,
             reason_code,
             scheme,
             will_attempt_retry,
             mandate,
             new_customer_bank_account,
             new_mandate,
             organisation,
             parent_event,
             payment,
             payout,
             previous_customer_bank_account,
             refund,
             subscription,
             timestamp
      FROM aws_fin_stage1_extracts.fin_go_cardless_api_events
      where resource_type = 'subscriptions');

SELECT id,
       created_at,
       resource_type,
       action,
       customer_notifications,
       cause,
       description,
       origin,
       reason_code,
       scheme,
       will_attempt_retry,
       mandate,
       new_customer_bank_account,
       new_mandate,
       organisation,
       parent_event,
       payment,
       payout,
       previous_customer_bank_account,
       refund,
       subscription,
       timestamp
FROM aws_fin_stage1_extracts.fin_go_cardless_api_events
where resource_type = 'subscriptions'
  and action = 'cancelled'

select *
from aws_fin_stage1_extracts.fin_go_cardless_api_events
where resource_type = 'mandates'
  and action = 'cancelled'
  and created_at between '2020-02-17' and '2020-02-23'

select *
from aws_fin_stage1_extracts.fin_go_cardless_api_events
where resource_type = 'subscriptions'
  and action = 'amended'
  and created_at between '2020-02-17' and '2020-02-23'
--   and created_at between '2020-03-30 00:00:00'::timestamp-14 and '2020-04-05 23:59:59'::timestamp-14

select *
from ref_cdb_supply_contracts
where external_id in (92270, 124407, 117810)


select max(created_at)
from aws_fin_stage1_extracts.fin_go_cardless_api_events
where resource_type = 'subscriptions'
  and action = 'amended'


select max(created_at)
from aws_fin_stage1_extracts.fin_go_cardless_api_events
where resource_type = 'mandates'
  and action = 'cancelled'