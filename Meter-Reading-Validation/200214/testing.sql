select rmp.account_id,
       rmp.meter_point_id,
       rr.register_id,
       rmp.meterpointtype,
       nvl(ea_elec.register_id, ea_gas.register_id) is not null                     as has_row,
       nvl(ea_elec.ind_estimated_advance, ea_gas.ind_estimated_advance) is not null as has_value,
       nvl(ea_elec.ind_estimated_advance, ea_gas.ind_estimated_advance)             as ea,
       nvl(ea_elec.last_reading_date, ea_gas.last_reading_date)                     as lrd,
       nvl(ea_elec.last_reading_value, ea_gas.last_reading_value)                   as lrv,
       case meterpointtype
           when 'E' then dcf.elec_reg_status
           when 'G' then dcf.gas_reg_status
           end                                                                      as supply_status
from ref_meterpoints rmp
         inner join ref_meters rm on rmp.account_id = rm.account_id and rmp.meter_point_id = rm.meter_point_id and
                                     rm.removeddate is null
         inner join ref_registers rr on rr.account_id = rm.account_id and rr.meter_id = rm.meter_id and
                                        rr.registers_tprperioddescription is not null
         left join temp_estimated_advance_elec ea_elec
                   on ea_elec.account_id = rr.account_id and ea_elec.register_id = rr.register_id and
                      rmp.meterpointtype = 'E'
         left join temp_estimated_advance_gas ea_gas
                   on ea_gas.account_id = rr.account_id and ea_gas.register_id = rr.register_id and
                      rmp.meterpointtype = 'G'
         left join ref_calculated_daily_customer_file dcf on rmp.account_id = dcf.account_id
where nvl(least(rmp.supplyenddate, rmp.associationenddate), getdate() + 1) > getdate()
  and ((lrd is null and ea is not null) or (lrd is not null and ea is null) or not has_row)
order by ea_elec.account_id, ea_elec.register_id


select *
from temp_estimated_advance_gas
where account_id = 1863


select rmp.account_id,
       rr.register_id,
       rmp.meterpointtype,
       nvl(ea_elec.ind_estimated_advance, ea_gas.ind_estimated_advance) as ea
from ref_meterpoints rmp
         inner join ref_meters rm on rmp.account_id = rm.account_id and rmp.meter_point_id = rm.meter_point_id and
                                     rm.removeddate is null
         inner join ref_registers rr on rr.account_id = rm.account_id and rr.meter_id = rm.meter_id and
                                        rr.registers_tprperioddescription is not null
         left join ref_estimated_advance_elec ea_elec
                   on ea_elec.account_id = rr.account_id and ea_elec.register_id = rr.register_id and
                      rmp.meterpointtype = 'E'
         left join ref_estimated_advance_gas ea_gas
                   on ea_gas.account_id = rr.account_id and ea_gas.register_id = rr.register_id and
                      rmp.meterpointtype = 'G'
where nvl(least(rmp.supplyenddate, rmp.associationenddate), getdate() + 1) > getdate()
  and (nvl(ea_elec.register_id, ea_gas.register_id) is null /*or ea is null*/)
order by account_id, register_id


select distinct meterreadingsourceuid, meterreadingtypeuid
from ref_readings_internal_valid

select *
from temp_estimated_advance_gas
where igl_estimated_advance = 0

select account_id,
       meterpointnumber,
       meter_point_id,
       register_id,
       meterreadingcreateddate,
       meterreadingdatetime,
       datediff(days, meterreadingcreateddate, meterreadingdatetime) as days_in_future,
       meterreadingsourceuid,
       meterreadingtypeuid,
       meterreadingstatusuid
from ref_readings_internal_valid
where days_in_future > 0
order by days_in_future desc