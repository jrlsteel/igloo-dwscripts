drop table temp_dmre_1275;
create table temp_dmre_1275 as
with amendment_info as (select gc_ev.id                                                             as event_id,
                               gc_ev.created_at                                                     as event_date,
                               gc_ev.subscription                                                   as sub_id,
                               tsa.amount                                                           as sub_new_amount,
                               id_map.igl_acc_id                                                    as ensek_id,
                               sc.id                                                                as supply_contract_id,
                               atas.created_at                                                      as last_pa_date,
                               atas.status                                                          as last_pa_state,
                               aset.nextbilldate::timestamp                                         as next_bill_date,
                               aset.billdayofmonth::int                                             as bill_day_of_month,
                               bill_day_of_month + 1                                                as pa_day_of_month,
                               asus.effective_from::timestamp                                       as suspended_from,
                               asus.effective_to::timestamp                                         as suspended_until,
                               dcf.acc_ssd,
                               dcf.acc_ed,
                               greatest(dateadd(month, 6, dcf.acc_ssd), getdate(), suspended_until) as pa_unlocked,
                               dateadd(month, ((date_part('day', pa_unlocked) > pa_day_of_month)::int),
                                       date_trunc('month', pa_unlocked) + pa_day_of_month - 1)      as next_possible_pa_date,
                               case
                                   when nvl(last_pa_state, 'exception') = 'exception' then
                                       next_possible_pa_date
                                   else greatest(next_possible_pa_date, dateadd(month, 3, last_pa_date))
                                   end                                                              as next_selected_pa_date
                        from aws_fin_stage1_extracts.fin_go_cardless_api_events gc_ev
                                 left join aws_fin_stage1_extracts.fin_go_cardless_api_subscriptions/*_files*/ gc_sub
                                           on gc_ev.subscription = gc_sub.id
                                 left join temp_sub_amounts tsa on tsa.id = gc_sub.id
                                 left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates/*_files*/ gc_man
                                           on gc_man.mandate_id = gc_sub.mandate
                                 left join vw_gocardless_customer_id_mapping id_map
                                           on gc_man.customerid = id_map.client_id
                                 left join ref_cdb_supply_contracts sc on id_map.igl_acc_id = sc.external_id
                                 left join (select *
                                            from (select *,
                                                         row_number()
                                                         over (partition by supply_contract_id order by created_at desc) as rn
                                                  from ref_cdb_adequacy_tasks) ordered_pa_tasks
                                            where rn = 1) atas on sc.id = atas.supply_contract_id
                                 left join aws_s3_stage2_extracts.stage2_cdbadequacysuspensions asus
                                           on sc.id = asus.supply_contract_id and
                                              getdate() between effective_from and nvl(effective_to, getdate() + 1)
                                 left join aws_s3_stage2_extracts.stage2_accountsettings aset
                                           on aset.account_id = sc.external_id
                                 left join ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
                        where gc_ev.resource_type = 'subscriptions'
                          and gc_ev.action = 'amended'
                          and gc_ev.created_at::timestamp >= '2020-03-19'),
     prev_sub_amount as (select *
                         from (select ai.sub_id,
                                      ai.event_date,
                                      p.amount,
                                      row_number()
                                      over (partition by ai.sub_id, ai.event_date order by p.created_at::timestamp desc) as rn
                               from amendment_info ai
                                        left join aws_fin_stage1_extracts.fin_go_cardless_api_payments p
                                                  on p.subscription = ai.sub_id and
                                                     p.created_at::timestamp < ai.event_date::timestamp) ordered
                         where rn = 1)
select ai.ensek_id,
       ai.event_date            as dd_change_date,
       psd.amount               as dd_before_change,
       ai.sub_new_amount        as dd_after_change,
       ai.next_selected_pa_date as next_pa_date,
       ai.acc_ssd,
       ai.acc_ed,
       ai.suspended_from,
       ai.suspended_until
from amendment_info ai
         left join prev_sub_amount psd on ai.sub_id = psd.sub_id and ai.event_date = psd.event_date

select distinct resource_type
from aws_fin_stage1_extracts.fin_go_cardless_api_events gc_ev


select *
from aws_fin_stage1_extracts.fin_go_cardless_api_payments


--          left join (select max(created_at) as date
--                     from aws_fin_stage1_extracts.fin_go_cardless_api_payments
--                     where subscription = gc_ev.subscription
--                       and created_at::timestamp < gc_ev.created_at::timestamp) ppd on true
--          left join aws_fin_stage1_extracts.fin_go_cardless_api_payments prev_pay
--                    on prev_pay.subscription = gc_ev.subscription and prev_pay.created_at = ppd.date


select date_part(day, getdate())

select acc_ssd, monthly_bill_date
from ref_calculated_daily_customer_file


select distinct status
from ref_cdb_adequacy_tasks

select *
from ref_cdb_adequacy_tasks
where status in ('completed', 'no_action')

select *

from temp_dmre_1275 full_table
         full join temp_dmre_1275_2 approx_table on full_table.ensek_id = approx_table.ensek_id and
                                                    full_table.dd_change_date = approx_table.dd_change_date
where full_table.ensek_id is null
   or approx_table.ensek_id is null


select ensek_id,
       dd_change_date::timestamp,
       dd_before_change,
       dd_after_change,
       next_pa_date,
       acc_ssd,
       acc_ed,
       suspended_from,
       suspended_until
from temp_dmre_1275
where dd_after_change != dd_before_change
order by ensek_id

select greatest(dateadd(month, -6, getdate()), getdate(), null)


create table temp_dmre_1275_interim as
select gc_ev.id                                                             as event_id,
       gc_ev.created_at                                                     as event_date,
       gc_ev.subscription                                                   as sub_id,
       gc_sub.amount                                                        as sub_new_amount,
       id_map.igl_acc_id                                                    as ensek_id,
       sc.id                                                                as supply_contract_id,
       atas.created_at                                                      as last_pa_date,
       atas.status                                                          as last_pa_state,
       aset.nextbilldate::timestamp                                         as next_bill_date,
       aset.billdayofmonth::int                                             as bill_day_of_month,
       bill_day_of_month + 1                                                as pa_day_of_month,
       asus.effective_from::timestamp                                       as suspended_from,
       asus.effective_to::timestamp                                         as suspended_until,
       dcf.acc_ssd,
       dcf.acc_ed,
       greatest(dateadd(month, 6, dcf.acc_ssd), getdate(), suspended_until) as pa_unlocked,
       dateadd(month, ((date_part('day', pa_unlocked) > pa_day_of_month)::int),
               date_trunc('month', pa_unlocked) + pa_day_of_month - 1)      as next_possible_pa_date,
       case
           when nvl(last_pa_state, 'exception') = 'exception' then
               next_possible_pa_date
           else greatest(next_possible_pa_date, dateadd(month, 3, last_pa_date))
           end                                                              as next_selected_pa_date
from aws_fin_stage1_extracts.fin_go_cardless_api_events gc_ev
         left join aws_fin_stage1_extracts.fin_go_cardless_api_subscriptions/*_files*/ gc_sub
                   on gc_ev.subscription = gc_sub.id
         left join aws_fin_stage1_extracts.fin_go_cardless_api_mandates/*_files*/ gc_man
                   on gc_man.mandate_id = gc_sub.mandate
         left join vw_gocardless_customer_id_mapping id_map
                   on gc_man.customerid = id_map.client_id
         left join ref_cdb_supply_contracts sc on id_map.igl_acc_id = sc.external_id
         left join (select *
                    from (select *,
                                 row_number()
                                 over (partition by supply_contract_id order by created_at desc) as rn
                          from ref_cdb_adequacy_tasks) ordered_pa_tasks
                    where rn = 1) atas on sc.id = atas.supply_contract_id
         left join aws_s3_stage2_extracts.stage2_cdbadequacysuspensions asus
                   on sc.id = asus.supply_contract_id and
                      getdate() between effective_from and nvl(effective_to, getdate() + 1)
         left join aws_s3_stage2_extracts.stage2_accountsettings aset
                   on aset.account_id = sc.external_id
         left join ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
where gc_ev.resource_type = 'subscriptions'
  and gc_ev.action = 'amended'
  and gc_ev.created_at::timestamp >= '2020-03-19'

create table temp_sub_amounts as
select id, amount
from aws_fin_stage1_extracts.fin_go_cardless_api_subscriptions_files
where id in (select sub_id from temp_dmre_1275_interim)