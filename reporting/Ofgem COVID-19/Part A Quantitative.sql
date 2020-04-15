select account_portfolio_summaries.*

from (
         select sum(acc_live::int)                                                       as num_on_supply,
                sum(has_active_subscription::int)                                        as total_dd_instructions,
                sum(occ_acc_sunday::int)                                                 as occ_accs_sunday,
                sum((man_canc_in_window and psr)::int)                                   as psr_dd_cancellations,
                sum((man_canc_in_window)::int)                                           as dd_cancellations,
                sum(((current_dd < monthly_usage_sterling) and dd_amended and psr)::int) as psr_low_dd,
                sum(((current_dd < monthly_usage_sterling) and dd_amended)::int)         as low_dd,
                sum((dd_failure and psr)::int)                                           as psr_dd_fail,
                sum(dd_failure::int)                                                     as dd_fail,
                sum((payment_failed_in_week and psr)::int)                               as psr_one_off_pay_fail,
                sum(payment_failed_in_week::int)                                         as one_off_pay_fail,
                sum(((current_dd >= monthly_usage_sterling) and psr)::int)::double precision /
                sum(psr::int)::double precision                                          as psr_correct_dd,
                sum((current_dd >= monthly_usage_sterling)::int)::double precision /
                num_on_supply::double precision                                          as correct_dd,
                monday
         from (
                  with date_range as (select '2020-04-06 00:00:00'::timestamp as monday,
                                             '2020-04-12 23:59:59'::timestamp as sunday)
                  select dcf.account_id,
                         up.user_id,
                         dcf.account_status != 'Cancelled' and
                         dcf.acc_ssd <= date_range.sunday and
                         nvl(acc_ed, getdate()) >= date_range.sunday                               as acc_live,
                         dcf.days_as_occ_acc is not null and
                         date_range.sunday between dcf.acc_ssd and (dcf.acc_ssd + days_as_occ_acc) as occ_acc_sunday,
                         attr_psr.attribute_custom_value is not null                               as psr,
                         ((num_elec_mpns * vlr.elec_sc * 365) + (eac_igloo_ca * vlr.elec_ur)) * 0.01 * 1.05 /
                         12                                                                        as elec_monthly_usage_sterling,
                         ((num_gas_mpns * vlr.gas_sc * 365) + (aq_igloo_ca * vlr.gas_ur)) * 0.01 * 1.05 /
                         12                                                                        as gas_monthly_usage_sterling,
                         nvl(elec_monthly_usage_sterling, 0) +
                         nvl(gas_monthly_usage_sterling, 0)                                        as monthly_usage_sterling,
                         greatest(nvl(square_payments.max_success, 0),
                                  nvl(stripe_payments.max_success, 0))                             as max_successful_payment,
                         greatest(nvl(square_payments.max_failed, 0),
                                  nvl(stripe_payments.max_failed, 0))                              as max_declined_payment,
                         max_successful_payment < max_declined_payment                             as payment_failed_in_week,
                         nvl(gc_info.has_active_subscriptions::int, 0)                             as has_active_subscription,
                         nvl(gc_info.subscription_amount, 0)                                       as current_dd,
                         nvl(gc_info.dd_fail, false)                                               as dd_failure,
                         nvl(man_canc_in_window and (has_active_subscription = 0), false)          as man_canc_in_window,
                         nvl(sub_updated_in_window, false)                                         as dd_amended,
                         date_range.monday
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
                           left join (select date_trunc('week', created_at::timestamp)                  as week_commencing,
                                             reference_id                                               as supply_contract_id,
                                             max(case when status = 'declined' then amount else 0 end)  as max_failed,
                                             max(case when status = 'completed' then amount else 0 end) as max_success
                                      from aws_s3_stage2_extracts.stage2_cdbcustomerpayments
                                      where reference_type = 'App\\SupplyContract'
                                      group by supply_contract_id, week_commencing) stripe_payments
                                     on stripe_payments.supply_contract_id = sc.id and
                                        stripe_payments.week_commencing = date_range.monday
                           left join (select date_trunc('week', created_at::timestamp)                  as week_commencing,
                                             case
                                                 when regexp_count(ensekid, '^[0-9]+$') = 1 then ensekid::int
                                                 else 0 end                                             as numeric_ensek_id,
                                             max(case when status = 'FAILED' then amount else 0 end)    as max_failed,
                                             max(case when status = 'COMPLETED' then amount else 0 end) as max_success
                                      from aws_fin_stage1_extracts.fin_square_api_payments
                                      where numeric_ensek_id != 0
                                      group by week_commencing, numeric_ensek_id) square_payments
                                     on square_payments.week_commencing = date_range.monday and
                                        square_payments.numeric_ensek_id = dcf.account_id
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
                                                         on all_ids.client_id = canc_man.customerid --and
--                                               canc_man.status = 'cancelled'
                                               left join aws_fin_stage1_extracts.fin_go_cardless_api_events man_cancellations
                                                         on man_cancellations.resource_type = 'mandates' and
                                                            man_cancellations.action = 'cancelled' and
                                                            man_cancellations.created_at::timestamp between date_range.monday and date_range.sunday and
                                                            man_cancellations.mandate = canc_man.mandate_id
                                      group by all_ids.igl_acc_id) gc_info
                                     on gc_info.igl_acc_id = dcf.account_id) account_figures
         where acc_live
         group by monday
     ) account_portfolio_summaries
-- left join ref_portfolio_metrics pm_


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

drop table if exists ref_portfolio_metric_types;
create table ref_portfolio_metric_types
(
    id                 bigint generated by default as identity (1, 1) not null,
    metric_name        varchar(50)                 not null,
    data_type          varchar(20)                 not null,
    metric_description varchar(200),
    introduced         timestamp default getdate() not null,
    updated            timestamp default getdate() not null,
    deprecated         timestamp,
    primary key (id)
)
    distkey (id)
    sortkey (id);
alter table ref_portfolio_metric_types
    owner to igloo;

drop table if exists ref_portfolio_metrics;
create table ref_portfolio_metrics
(
    id             bigint generated by default as identity (1, 1) not null,
    metric_type_id bigint                      not null,
    timespan_start timestamp                   not null,
    timespan_end   timestamp                   not null,
    metric_value   varchar(max)                not null,
    created_at     timestamp default getdate() not null,
    updated_at     timestamp default getdate() not null,
    primary key (metric_type_id, timespan_start, timespan_end),
    foreign key (metric_type_id) references ref_portfolio_metric_types (id)
)
    distkey ( metric_type_id )
    sortkey (metric_type_id, timespan_start);
alter table ref_portfolio_metrics
    owner to igloo;


INSERT INTO "public"."ref_portfolio_metrics" ("id", "metric_type_id", "timespan_start", "timespan_end", "metric_value",
                                              "created_at", "updated_at")
VALUES (DEFAULT, 1, '2020-03-30 00:00:00', '2020-03-30 23:59:59', '882', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-03-30 00:00:00', '2020-03-30 23:59:59', '96', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-03-31 00:00:00', '2020-03-31 23:59:59', '743', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-03-31 00:00:00', '2020-03-31 23:59:59', '69', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-01 00:00:00', '2020-04-01 23:59:59', '793', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-01 00:00:00', '2020-04-01 23:59:59', '51', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-02 00:00:00', '2020-04-02 23:59:59', '768', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-02 00:00:00', '2020-04-02 23:59:59', '66', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-03 00:00:00', '2020-04-03 23:59:59', '985', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-03 00:00:00', '2020-04-03 23:59:59', '79', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-04 00:00:00', '2020-04-04 23:59:59', '291', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-04 00:00:00', '2020-04-04 23:59:59', '16', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-05 00:00:00', '2020-04-05 23:59:59', '278', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-05 00:00:00', '2020-04-05 23:59:59', '13', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-06 00:00:00', '2020-04-06 23:59:59', '945', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-06 00:00:00', '2020-04-06 23:59:59', '44', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-07 00:00:00', '2020-04-07 23:59:59', '813', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-07 00:00:00', '2020-04-07 23:59:59', '25', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-08 00:00:00', '2020-04-08 23:59:59', '824', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-08 00:00:00', '2020-04-08 23:59:59', '30', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-09 00:00:00', '2020-04-09 23:59:59', '681', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-09 00:00:00', '2020-04-09 23:59:59', '31', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-10 00:00:00', '2020-04-10 23:59:59', '248', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-10 00:00:00', '2020-04-10 23:59:59', '9', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-11 00:00:00', '2020-04-11 23:59:59', '177', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-11 00:00:00', '2020-04-11 23:59:59', '4', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-12 00:00:00', '2020-04-12 23:59:59', '166', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-12 00:00:00', '2020-04-12 23:59:59', '12', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-13 00:00:00', '2020-04-13 23:59:59', '294', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-13 00:00:00', '2020-04-13 23:59:59', '8', DEFAULT, DEFAULT),
       (DEFAULT, 1, '2020-04-14 00:00:00', '2020-04-14 23:59:59', '911', DEFAULT, DEFAULT),
       (DEFAULT, 2, '2020-04-14 00:00:00', '2020-04-14 23:59:59', '37', DEFAULT, DEFAULT)

