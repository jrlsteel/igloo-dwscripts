with weekly_portfolio_stats as (
    select metric_type_id,
           date_trunc('week', timespan_start)     week_commencing,
           sum(metric_value::double precision) as total
    from ref_portfolio_metrics
    where metric_type_id between 1 and 6
    group by metric_type_id, week_commencing
)
select account_portfolio_summaries.*,
       zendesk_tickets.total::int                                            as num_zendesk_tickets,
       case
           when zendesk_tickets.total is null then null
           else round(zendesk_covid.total * 100 / zendesk_tickets.total) end as perc_covid_tickets,
       num_energy_theft.total::int                                           as num_energy_theft,
       round(value_energy_theft.total, 2)                                    as value_energy_theft,
       eng_visits.total::int                                                 as num_emergency_visits,
       failed_visits.total::int                                              as covid_failed_visits
from (select monday                                                                   as week_commencing,
             sum(acc_live::int)                                                       as num_on_supply,
             sum(has_active_subscription::int)                                        as total_dd_instructions,
             sum(occ_acc_sunday::int)                                                 as occ_accs_sunday,
             sum((man_canc_in_window and psr)::int)                                   as psr_dd_cancellations,
             sum((man_canc_in_window)::int)                                           as dd_cancellations,
             sum(((current_dd < monthly_usage_sterling) and dd_amended and psr)::int) as psr_low_dd,
             sum(((current_dd < monthly_usage_sterling) and dd_amended)::int)         as low_dd,
             sum((dd_failure and psr)::int)                                           as psr_dd_fail,
             sum(dd_failure::int)                                                     as dd_fail,
             round(sum(((current_dd >= monthly_usage_sterling) and psr)::int)::double precision * 100 /
                   nullif(sum(psr::int), 0)::double precision)                        as psr_correct_dd,
             round(sum((current_dd >= monthly_usage_sterling)::int)::double precision * 100 /
                   nullif(num_on_supply, 0)::double precision)                        as correct_dd,
             sum((occ_acc_sunday and first_bill_date <= dateadd(month, -1, sunday) and
                  not pay_in_month)::int)                                             as occ_acc_pay_fail,
             sum((psr and occ_acc_sunday and first_bill_date <= dateadd(month, -1, sunday) and
                  not pay_in_month)::int)                                             as psr_occ_acc_pay_fail,
             sum(credit)                                                              as customer_credit_sunday,
             sum(arrears)                                                             as customer_arrears_sunday
      from (with date_range as (select dateadd(second, -1, date) as sunday, date_trunc('week', sunday) as monday
                                from ref_date
                                where day_name = 'Monday'
                                  and date between '2016-01-01' and getdate())
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
                   nvl(gc_info.has_active_subscriptions::int, 0)                             as has_active_subscription,
                   nvl(gc_info.subscription_amount, 0)                                       as current_dd,
                   nvl(gc_info.dd_fail, false)                                               as dd_failure,
                   nvl(man_canc_in_window and (has_active_subscription = 0), false)          as man_canc_in_window,
                   nvl(sub_updated_in_window, false)                                         as dd_amended,
                   date_range.monday,
                   date_range.sunday,
                   dcf.first_bill_date,
                   dcf.latest_bill_date,
                   nvl(payment_in_month.num_payments, 0) > 0                                 as pay_in_month,
                   greatest(0, acc_balance.currentbalance)                                   as arrears,
                   least(0, acc_balance.currentbalance)                                      as credit
            from ref_calculated_daily_customer_file dcf
                     left join date_range on true
                     left join (select tran_ids.account_id,
                                       tran_ids.sunday,
                                       rat.currentbalance
                                from (select rat.account_id,
                                             sun.sunday,
                                             max(rat.id) as latest_transaction
                                      from (select dateadd('second', -1, date) sunday
                                            from ref_date
                                            where day_name = 'Monday'
                                              and date between '2016-01-01' and getdate()) sun
                                               inner join ref_account_transactions rat
                                                          on rat.creationdetail_createddate::timestamp < sun.sunday
                                      group by rat.account_id, sun.sunday) tran_ids
                                         left join ref_account_transactions rat on tran_ids.latest_transaction = rat.id) acc_balance
                               on acc_balance.account_id = dcf.account_id and acc_balance.sunday = date_range.sunday
                     left join (select rat.account_id,
                                       sun.date as month_ending,
                                       count(*) as num_payments
                                from ref_account_transactions rat
                                         inner join (select date, dateadd('month', -1, date) prev_month
                                                     from ref_date
                                                     where day_name = 'Sunday'
                                                       and date between '2016-01-01' and getdate()) sun
                                                    on rat.transactiontype != 'INTEREST' and rat.amount < 0 and
                                                       trunc(rat.creationdetail_createddate::timestamp) between sun.prev_month and sun.date
                                group by rat.account_id, sun.date) payment_in_month
                               on payment_in_month.account_id = dcf.account_id
                                   and payment_in_month.month_ending = trunc(date_range.sunday)
                     left join ref_cdb_supply_contracts sc on dcf.account_id = sc.external_id
                     left join ref_cdb_user_permissions up
                               on up.permissionable_type = 'App\\SupplyContract' and
                                  up.permissionable_id = sc.id and
                                  up.permission_level = 0
                     left join ref_cdb_attributes attr_psr on attr_psr.attribute_type_id = 17 and
                                                              attr_psr.attribute_custom_value != '[]' and
                                                              attr_psr.effective_from <= date_range.sunday and
                                                              nvl(attr_psr.effective_to, getdate() + 1000) > date_range.sunday and
                                                              attr_psr.entity_id = sc.id
                     left join vw_latest_rates_ensek vlr on vlr.account_id = dcf.account_id
                     left join (select igl_acc_id,
                                       date_range.monday,
                                       date_range.sunday,
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
                                group by all_ids.igl_acc_id, date_range.monday, date_range.sunday) gc_info
                               on gc_info.igl_acc_id = dcf.account_id and
                                  date_range.sunday = gc_info.sunday) account_figures
      where acc_live
      group by monday
     ) account_portfolio_summaries
         left join weekly_portfolio_stats zendesk_tickets on zendesk_tickets.metric_type_id = 1 and
                                                             zendesk_tickets.week_commencing =
                                                             account_portfolio_summaries.week_commencing
         left join weekly_portfolio_stats zendesk_covid on zendesk_covid.metric_type_id = 2 and
                                                           zendesk_covid.week_commencing =
                                                           account_portfolio_summaries.week_commencing
         left join weekly_portfolio_stats num_energy_theft on num_energy_theft.metric_type_id = 3 and
                                                              num_energy_theft.week_commencing =
                                                              account_portfolio_summaries.week_commencing
         left join weekly_portfolio_stats value_energy_theft on value_energy_theft.metric_type_id = 4 and
                                                                value_energy_theft.week_commencing =
                                                                account_portfolio_summaries.week_commencing
         left join weekly_portfolio_stats eng_visits on eng_visits.metric_type_id = 5 and
                                                        eng_visits.week_commencing =
                                                        account_portfolio_summaries.week_commencing
         left join weekly_portfolio_stats failed_visits on failed_visits.metric_type_id = 6 and
                                                           failed_visits.week_commencing =
                                                           account_portfolio_summaries.week_commencing
order by week_commencing desc


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

alter table vw_gocardless_customer_id_mapping
    owner to igloo

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

