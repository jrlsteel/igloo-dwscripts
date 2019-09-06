select mp.account_id,
       mp.meter_point_id,
       mp.meterpointnumber,
       met.meter_id,
       met.meterserialnumber,
       reg.register_id,
       reg.registers_registerreference,

       mp.meterpointtype,
       mp.associationstartdate,
       mp.supplystartdate,
       mp.associationenddate,
       mp.supplyenddate,
       met.installeddate,
       met.removeddate,
       reg.registers_tpr,
       reg.registers_eacaq
from ref_registers reg
left join ref_meters met on reg.account_id = met.account_id and reg.meter_id = met.meter_id
left join ref_meterpoints mp on met.account_id = mp.account_id and met.meter_point_id = mp.meter_point_id
where reg.account_id = 1831