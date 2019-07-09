-- Remove duplicates
select distinct account_id,
                meter_point_id,
                meter_id,
                register_id,
                registers_eacaq,
                registers_registerreference,
                registers_sourceidtype,
                registers_tariffcomponent,
                registers_tpr,
                registers_tprperioddescription
from ref_registers;

select distinct account_id,
                meter_point_id,
                meter_id,
                meterserialnumber,
                installeddate,
                removeddate
from ref_meters;

select distinct account_id,
                meter_point_id,
                meterpointnumber,
                associationstartdate,
                associationenddate,
                supplystartdate,
                supplyenddate,
                issmart,
                issmartcommunicating,
                meterpointtype
from ref_meterpoints
where greatest(associationstartdate,supplystartdate) <= least(associationenddate,supplyenddate) or
      nvl(associationenddate,supplyenddate) isnull -- non-cancelled
