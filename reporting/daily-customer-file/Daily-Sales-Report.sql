select sc.id                                                                               as supply_contract_id,
       sc.external_id                                                                      as ensek_id,
       q.channel                                                                           as signup_channel,
       q.secondary_channel,
       bm.map_name                                                                         as broker_name,
       bs.broker_urn,
       sc.created_at                                                                       as signup_date,
       case q.campaign_id
           when 1 then 'Broker signups from Quotezone.'
           when 2 then 'Broker signups from MoneySuperMarket.'
           when 3 then 'Promoting our Facebook Trustpilot score.'
           when 4 then 'Broker signups from FirstHelpline.'
           when 5 then 'Broker signups from Dixons Carphone Warehouse.'
           when 6 then 'Broker signups from Energylinx.'
           when 7 then 'Refer a Friend scheme'
           when 8 then 'Promotion for EV 1200 free miles scheme'
           when 9
               then 'Customers signing up through the TeenTech link will receive a £100 reward.'
           when 10 then 'Broker signups from MoneyExpert.'
           when 11 then 'Campaign for tracking signups during the Christmas lights sponsorship'
           when 12 then 'Campaign for tracking signups via radio adverts during the Christmas lights sponsorship'
           when 13 then 'Campaign for tracking signups via QR code during the Christmas lights sponsorship'
           else q.campaign_id::varchar(3)
           end                                                                             as campaign,
       case when acc_stat.reg_status is null then 'Cancelled' else acc_stat.reg_status end as account_status,
       case
           when account_status = 'Cancelled' then
               coalesce(nullif(bs.status, 'Success'), cancelled_mps.rma_supply_status, 'Generic Cancellation')
           else null end                                                                   as cancellation_reason,
        q.fuel_type as supply_type
from ref_cdb_supply_contracts sc
         left join ref_cdb_registrations r on sc.registration_id = r.id
         left join ref_cdb_quotes q on q.id = r.quote_id
         left join ref_cdb_broker_maps bm on bm.campaign_id = q.campaign_id
         left join ref_cdb_broker_signups bs on bs.registration_id = sc.registration_id and bs.id != 846
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