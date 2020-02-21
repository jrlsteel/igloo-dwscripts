/*
 1) Every live register has an entry in the relevant (gas / elec) est adv table
 Expected result: empty result set
 */
select rmp.account_id,
       rr.register_id,
       rmp.meterpointtype
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
  and (nvl(ea_elec.register_id, ea_gas.register_id) is null)
order by account_id, register_id;


/*
 2) Every row with a "latest meter reading" and aq/eac has an estimated advance
 Expected result: empty result sets
 */
select *
from ref_estimated_advance_elec
where last_reading_date is not null
  and ind_eac is not null
  and ind_estimated_advance is null;
select *
from ref_estimated_advance_gas
where last_reading_date is not null
  and ind_aq is not null
  and ind_estimated_advance is null;


/*
 3) Every row missing a "latest meter reading" and/or aq/eac has a null estimated advance
 Expected result: empty result sets
 */
select *
from ref_estimated_advance_elec
where (last_reading_date is null
    or ind_eac is null)
  and ind_estimated_advance is not null;
select *
from ref_estimated_advance_gas
where (last_reading_date is null
    or ind_aq is null)
  and ind_estimated_advance is not null;


/*
 4) No estimated readings are present in RRIV
 Expected result: empty result set
 */
select *
from ref_readings_internal_valid
where meterreadingtypeuid = 'ESTIMATED'
   or meterreadingsourceuid = 'ESTIMATE'


/*
 5) All current valid reading types are present in RRIV
 Expected result: meterreadingsourceuid
                  DCOPENING
                  CUSTOMER
                  CUSTOMERMOVEIN
                  SMART
                  DC
                  CUSTOMERMOVEOUT
                  VERBALLYAGREED
 */
select distinct meterreadingsourceuid
from ref_readings_internal_valid