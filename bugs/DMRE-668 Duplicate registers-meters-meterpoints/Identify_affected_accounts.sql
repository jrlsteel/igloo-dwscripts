select
*
from (
         (select account_id, sum(dups) as num_records_reg, count(dups) as num_unique_reg
         from (
                  select account_id,
                         meter_point_id,
                         meter_id,
                         register_id,
                         registers_eacaq,
                         registers_registerreference,
                         registers_sourceidtype,
                         registers_tariffcomponent,
                         registers_tpr,
                         registers_tprperioddescription,
                         count(*) as dups
                  from ref_registers
                  group by account_id, meter_point_id, meter_id, register_id, registers_eacaq,
                           registers_registerreference,
                           registers_sourceidtype, registers_tariffcomponent, registers_tpr,
                           registers_tprperioddescription
                  having dups > 1
              ) duplicated_ref_registers
         group by account_id) reg
full outer join
(select account_id, sum(dups) as num_records_met, count(dups) as num_unique_met
from (
         select account_id,
                meter_point_id,
                meter_id,
                meterserialnumber,
                installeddate,
                removeddate,
                count(*) as dups
         from ref_meters
         group by account_id, meter_point_id, meter_id, meterserialnumber, installeddate, removeddate
         having dups > 1
     ) duplicated_ref_meters
group by account_id) met
    on reg.account_id = met.account_id
full outer join
(select account_id, sum(dups) as num_records_mps, count(dups) as num_unique_mps
from (
         select account_id,
                meter_point_id,
                meterpointnumber,
                associationstartdate,
                associationenddate,
                supplystartdate,
                supplyenddate,
                issmart,
                issmartcommunicating,
                meterpointtype,
                count(*) as dups
         from ref_meterpoints
         group by account_id, meter_point_id, meterpointnumber, associationstartdate, associationenddate,
                  supplystartdate, supplyenddate, issmart, issmartcommunicating, meterpointtype
         having dups > 1
     ) duplicated_ref_meterpoints
group by account_id) mps
    on mps.account_id = reg.account_id
)