-- truncate table temp_estimated_advance_elec
-- insert into ref_estimated_advance_elec
-- create table temp_estimated_advance_elec as
select x1.account_id,
       x1.register_id,
       x1.last_reading_date,
       x1.last_reading_value,
       x1.eac                                                      as igl_eac,
       x1.ind_eac,
       x1.effective_for,
       x1.igl_estimated_advance,
       x1.ind_estimated_advance,
       coalesce(tol_elec.tol_min * igl_estimated_advance / 100, 0) as igl_lower_threshold,
       coalesce(tol_elec.tol_max * igl_estimated_advance / 100, 0) as igl_higher_threshold,
       coalesce(tol_elec.tol_min * ind_estimated_advance / 100, 0) as ind_lower_threshold,
       coalesce(tol_elec.tol_max * ind_estimated_advance / 100, 0) as ind_higher_threshold,
       coalesce(rra.registersattributes_attributevalue::int, 5)    as register_num_digits,
       getdate()                                                   as etlchange
from (select account_id,
             register_id,
             last_reading_date,
             last_reading_value,
             days_since_last_read,
             round(coalesce(eac, 0), 0)                     as eac,
             round(coalesce(igl_ind_eac, 0), 0)             as igl_ind_eac,
             round(coalesce(ind_eac, 0), 0)                 as ind_eac,
             getdate()                                      as effective_for,
             eac * nvl(ppc, days_since_last_read / 365)     as igl_estimated_advance,
             ind_eac * nvl(ppc, days_since_last_read / 365) as ind_estimated_advance
      from (select mp.account_id                                                        as account_id,
                   r.register_id,
                   latest_read.meterreadingdatetime                                      as last_reading_date,
                   latest_read.readingvalue                                              as last_reading_value,
                   datediff(days, latest_read.meterreadingdatetime, trunc(getdate()))    as days_since_last_read,
                   coalesce(eac.igl_ind_eac, 0)                                          as igl_ind_eac,
                   coalesce(r.registers_eacaq, 0)                                        as ind_eac,
                   coalesce(nullif(eac.igl_ind_eac, 0), nullif(r.registers_eacaq, 0), 0) as eac,
                   (select sum(ppc_sum) / nullif(count(ppc_sum), 0)
                    from (select * from ref_d18_igloo_ppc union select * from ref_d18_igloo_ppc_forecast) igl_ppc
                    where gsp_group_id = rma_gsp.attributes_attributevalue
                      and ss_conf_id = rma_ssc.attributes_attributevalue
                      and cast(time_pattern_regime as bigint) = r.registers_tpr
                      and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                      and st_date >= trunc(latest_read.meterreadingdatetime)
                      and st_date < trunc(current_date)
                    group by gsp_group_id, ss_conf_id) * days_since_last_read            as ppc
            from ref_meterpoints mp
                     inner join ref_meters m
                                on m.account_id = mp.account_id and m.meter_point_id = mp.meter_point_id and
                                   m.removeddate is null
                     inner join ref_registers r on r.account_id = mp.account_id and r.meter_id = m.meter_id and
                                                   r.registers_tprperioddescription is not null
                     left join ref_meterpoints_attributes rma_gsp
                               on rma_gsp.account_id = mp.account_id and
                                  rma_gsp.meter_point_id = mp.meter_point_id and
                                  rma_gsp.attributes_attributename = 'GSP'
                     left join ref_meterpoints_attributes rma_ssc
                               on rma_ssc.account_id = mp.account_id and
                                  mp.meter_point_id = rma_ssc.meter_point_id and
                                  rma_ssc.attributes_attributename = 'SSC'
                     left join ref_meterpoints_attributes rma_pcl
                               on rma_pcl.account_id = mp.account_id and
                                  mp.meter_point_id = rma_pcl.meter_point_id and
                                  rma_pcl.attributes_attributename = 'Profile Class'
                     left join (select *
                                from (select account_id,
                                             register_id,
                                             meterreadingdatetime,
                                             readingvalue,
                                             row_number()
                                             over (partition by ri.account_id, register_id order by meterreadingdatetime desc) rownum
                                      from ref_readings_internal_valid ri
                                      order by ri.meterreadingdatetime desc) r1
                                where r1.rownum = 1) latest_read
                               on latest_read.account_id = mp.account_id and latest_read.register_id = r.register_id
                     left join ref_calculated_igl_ind_eac eac
                               on eac.account_id = mp.account_id and eac.register_id = r.register_id
            where mp.meterpointtype = 'E'
              and nvl(least(mp.associationenddate, mp.supplyenddate), getdate() + 1) > getdate()) x
      order by x.account_id, register_id) x1
         left join ref_stg_tolerances tol_elec
                   on tol_elec.tol_group = 'industry_tolerance_elec'
                       and tol_elec.group_id = 4
                       and tol_elec.lookup_key = 'ind_elec'
                       and (tol_elec.effective_to is null or tol_elec.effective_to >= getdate())
                       and tol_elec.effective_from <= getdate()
         left join ref_registers_attributes rra
                   on rra.account_id = x1.account_id and rra.register_id = x1.register_id and
                      registersattributes_attributename = 'No_Of_Digits'
order by x1.account_id, x1.register_id
;


-- ##########################################

select rmp.account_id,
       rmp.meter_point_id,
       rr.register_id,
       rmp.meterpointtype,
       nvl(ea_elec.ind_estimated_advance, ea_gas.ind_estimated_advance) as ea,
       ea_elec.last_reading_date,
       ea_elec.last_reading_value
from ref_meterpoints rmp
         inner join ref_meters rm on rmp.account_id = rm.account_id and rmp.meter_point_id = rm.meter_point_id and
                                     rm.removeddate is null
         inner join ref_registers rr on rr.account_id = rm.account_id and rr.meter_id = rm.meter_id and
                                        rr.registers_tprperioddescription is not null
         left join temp_estimated_advance_elec ea_elec
                   on ea_elec.account_id = rr.account_id and ea_elec.register_id = rr.register_id and
                      rmp.meterpointtype = 'E'
         left join ref_estimated_advance_gas ea_gas
                   on ea_gas.account_id = rr.account_id and ea_gas.register_id = rr.register_id and
                      rmp.meterpointtype = 'G'
where nvl(least(rmp.supplyenddate, rmp.associationenddate), getdate() + 1) > getdate()
  and (nvl(ea_elec.register_id, ea_gas.register_id) is null or ea is null)
and rmp.meterpointtype = 'E'
order by account_id, register_id

select rrim.meterreadingsourceuid as source, rrivm.meterreadingsourceuid is not null as valid_to_rriv
from (
         select distinct meterreadingsourceuid
         from ref_readings_internal
     ) rrim
    left join
    (
         select distinct meterreadingsourceuid
         from ref_readings_internal_valid
     ) rrivm on rrivm.meterreadingsourceuid = rrim.meterreadingsourceuid

select distinct meterreadingsourceuid, meterreadingtypeuid from ref_readings_internal


select *
from temp_estimated_advance_gas
where account_id = 1863

select *
from (select account_id,
             register_id,
             meterreadingdatetime,
             readingvalue,
             row_number()
             over (partition by ri.account_id, register_id order by meterreadingdatetime desc) rownum
      from ref_readings_internal_valid ri
      where account_id = 15116
      order by ri.meterreadingdatetime desc) r1
where r1.rownum = 1


select *
from ref_readings_internal_audit
where register_id = 196470
order by meterreadingdatetime desc

-- is gas opening reading for account 15116 and register 196470 in rriv now? Wasn't yday but was valid on Ensek

select * from ref_readings_internal where meterreadingsourceuid = 'ESTIMATE' and meterreadingtypeuid = 'ACTUAL'

select meterreadingsourceuid,
       meterreadingtypeuid,
       min(etlchange) as first_seen

from ref_readings_internal_audit
group by meterreadingtypeuid, meterreadingsourceuid