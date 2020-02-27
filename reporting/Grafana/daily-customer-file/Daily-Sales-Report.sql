create table temp_reporting_daily_sales as
select sc.id                                                                               as supply_contract_id,
       sc.external_id                                                                      as ensek_id,
       q.channel                                                                           as signup_channel,
       q.secondary_channel,
       bm.map_name                                                                         as broker_name,
       bs.broker_urn,
       coalesce(sc.created_at, bs.created_at)                                              as signup_date,
       camp.description                                                                    as campaign,
       case when acc_stat.reg_status is null then 'Cancelled' else acc_stat.reg_status end as account_status,
       case
           when account_status = 'Cancelled' then
               coalesce(nullif(bs.status, 'Success'), cancelled_mps.rma_supply_status, 'Generic Cancellation')
           else null end                                                                   as cancellation_reason,
       q.fuel_type                                                                         as supply_type,
       part.name                                                                           as partner_name,
       getdate()                                                                           as etlchange
from ref_cdb_supply_contracts sc
         left join ref_cdb_registrations r on sc.registration_id = r.id
         left join ref_cdb_quotes q on q.id = r.quote_id
         left join aws_s3_stage2_extracts.stage2_cdbcampaigns camp on q.campaign_id = camp.id
         left join ref_cdb_broker_maps bm on bm.campaign_id = q.campaign_id
         full join ref_cdb_broker_signups bs on bs.registration_id = sc.registration_id and bs.id != 846
         left join aws_s3_stage2_extracts.stage2_cdbpartners part on bs.partner_id = part.id
         left join (select account_id,
                           min(greatest(supplystartdate, associationstartdate)) as start_date,
                           nullif(max(coalesce(least(supplyenddate, associationenddate),
                                               current_date + 1000)),
                                  current_date + 1000)                          as end_date,
                           udf_meterpoint_status(start_date, end_date)          as reg_status
                    from ref_meterpoints
                    group by account_id) acc_stat on acc_stat.account_id = sc.external_id
         left join (select rmr.account_id,
                           listagg(distinct rma_ss.attributes_attributevalue, ',') as rma_supply_status
                    from ref_meterpoints_raw rmr
                             left join ref_meterpoints_attributes rma_ss
                                       on rmr.account_id = rma_ss.account_id and
                                          rmr.meter_point_id = rma_ss.meter_point_id and
                                          attributes_attributename = 'Supply_Status'
                                           and usage_flag = 'cancelled'
                    group by rmr.account_id) cancelled_mps on cancelled_mps.account_id = sc.external_id
order by supply_contract_id, ensek_id