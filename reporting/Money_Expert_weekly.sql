-- meterpoint level
select null                                         as number,
       right(bs.broker_urn, len(bs.broker_urn) - 3) as urn,
       bs.created_at                                as date_of_sale,
       dateadd(days, 21, sc.created_at)             as target_supply_date,
       rm.supplystartdate                           as actual_supply_date,
       addr.postcode                                as postcode,
       case
           when rm.supplyenddate is not null and rm.supplystartdate > rm.supplyenddate then 'Cancelled'
           when rm.supplystartdate > getdate() then 'Pending'
           else 'Live' end                          as account_status,
       'Money Expert Telesales'                     as sales_subchannel,
       case rm.meterpointtype
           when 'G' then 'GAS'
           when 'E' then 'ELEC'
           else null end                            as fuel,
       rm.meterpointnumber                          as meter_point_reference,
       bs.status                                    as contract_status,
       null                                         as cancellation_reason,
       null                                         as cancellation_notified_date,
       null                                         as payment_status,
       'Igloo Pioneer'                              as tariff_name
from ref_cdb_broker_signups bs
         left join ref_cdb_supply_contracts sc on bs.registration_id = sc.registration_id
         left join ref_meterpoints_raw rm on rm.account_id = sc.external_id
         left join ref_cdb_addresses addr on sc.supply_address_id = addr.id
where bs.partner_id = 9 -- Money Expert
  and datediff(weeks, date_of_sale, getdate()) < 8
order by urn, fuel

-- broker signup level
select null                                         as number,
       right(bs.broker_urn, len(bs.broker_urn) - 3) as urn,
       min(bs.created_at)                           as date_of_sale,
       dateadd(days, 21, min(sc.created_at))        as target_supply_date,
       min(rm.supplystartdate)                      as actual_supply_date,
       min(addr.postcode)                           as postcode,
       case min(case
                    when rm.supplyenddate is not null and rm.supplystartdate > rm.supplyenddate then 2
                    when rm.supplystartdate > getdate() then 1
                    else 0 end)
           when 2 then 'Cancelled'
           when 1 then 'Pending'
           when 0 then 'Live'
           end                                      as account_status,
       'Money Expert Telesales'                     as sales_subchannel,
       case listagg(rm.meterpointtype)
           when 'EG' then 'Dual'
           when 'GE' then 'Dual'
           when 'E' then 'Elec'
           when 'G' then 'Gas'
           end                                      as fuel,
       listagg(rm.meterpointnumber, ',')            as meter_point_reference,
       min(bs.status)                               as contract_status,
       null                                         as cancellation_reason,
       null                                         as cancellation_notified_date,
       null                                         as payment_status,
       'Igloo Pioneer'                              as tariff_name
from ref_cdb_broker_signups bs
         left join ref_cdb_supply_contracts sc on bs.registration_id = sc.registration_id
         left join ref_meterpoints_raw rm on rm.account_id = sc.external_id
         left join ref_cdb_addresses addr on sc.supply_address_id = addr.id
where bs.partner_id = 9 -- Money Expert
  and datediff(weeks, nvl(bs.created_at, '2018-01-01'), getdate()) < 8
group by urn
order by urn, fuel
