select sum(on_supply_0_3)     as on_supply_0_3,
       sum(acc_read_0_3)      as acc_read_0_3,
       sum(on_supply_3_6)     as on_supply_3_6,
       sum(acc_read_3_6)      as acc_read_3_6,
       sum(on_supply_6_9)     as on_supply_6_9,
       sum(acc_read_6_9)      as acc_read_6_9,
       sum(on_supply_9_12)    as on_supply_9_12,
       sum(acc_read_9_12)     as acc_read_9_12,
       sum(on_supply_12_plus) as on_supply_12_plus,
       sum(acc_read_12_plus)  as acc_read_12_plus
from (select account_id,
             max(on_supply_0_3)                                                                               as on_supply_0_3,
             max(on_supply_3_6)                                                                               as on_supply_3_6,
             max(on_supply_6_9)                                                                               as on_supply_6_9,
             max(on_supply_9_12)                                                                              as on_supply_9_12,
             max(on_supply_12_plus)                                                                           as on_supply_12_plus,
             case
                 when sum(has_read_0_3) = sum(on_supply_0_3) and sum(on_supply_0_3) > 0 then 1
                 else 0 end                                                                                   as acc_read_0_3,
             case
                 when sum(has_read_3_6) = sum(on_supply_3_6) and sum(on_supply_3_6) > 0 then 1
                 else 0 end                                                                                   as acc_read_3_6,
             case
                 when sum(has_read_6_9) = sum(on_supply_6_9) and sum(on_supply_6_9) > 0 then 1
                 else 0 end                                                                                   as acc_read_6_9,
             case
                 when sum(has_read_9_12) = sum(on_supply_9_12) and sum(on_supply_9_12) > 0 then 1
                 else 0 end                                                                                   as acc_read_9_12,
             case
                 when sum(has_read_12_plus) = sum(on_supply_12_plus) and sum(on_supply_12_plus) > 0 then 1
                 else 0 end                                                                                   as acc_read_12_plus
      from (select account_id,
                   register_id,
                   meter_point_id,
                   1                                                                    as on_supply_0_3,
                   case when max(register_age) >= 3 then 1 else 0 end                   as on_supply_3_6,
                   case when max(register_age) >= 6 then 1 else 0 end                   as on_supply_6_9,
                   case when max(register_age) >= 9 then 1 else 0 end                   as on_supply_9_12,
                   case when max(register_age) >= 12 then 1 else 0 end                  as on_supply_12_plus,
                   max(case when months_ago < 3 then 1 else 0 end)                      as has_read_0_3,
                   max(case when 3 <= months_ago and months_ago < 6 then 1 else 0 end)  as has_read_3_6,
                   max(case when 6 <= months_ago and months_ago < 9 then 1 else 0 end)  as has_read_6_9,
                   max(case when 9 <= months_ago and months_ago < 12 then 1 else 0 end) as has_read_9_12,
                   max(case when 12 <= months_ago then 1 else 0 end)                    as has_read_12_plus
            from (select rreg.account_id,
                         rreg.register_id,
                         rreg.meter_point_id,
                         datediff(months, greatest(rm.supplystartdate, rm.associationstartdate),
                                  current_date)                            as register_age,
                         datediff(months, meterreadingdatetime, getdate()) as months_ago
                  from ref_readings_internal_valid rriv
                           right join ref_registers rreg
                                      on rriv.account_id = rreg.account_id and
                                         rriv.register_id = rreg.register_id
                           inner join ref_meterpoints rm
                                      on rreg.meter_point_id = rm.meter_point_id and
                                         rreg.account_id = rm.account_id and
                                         greatest(rm.supplystartdate, rm.associationstartdate) <= current_date and
                                         nvl(least(rm.supplyenddate, rm.associationenddate), current_date + 1000) >=
                                         current_date
                 ) diffs
            group by account_id, register_id, meter_point_id
           ) register_level
      group by account_id
     ) account_level