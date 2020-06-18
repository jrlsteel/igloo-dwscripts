-- truncate table ref_eac_calc_params;
-- insert into ref_eac_calc_params
with cte_readings as (select account_id,
                             meter_point_id,
                             register_id,
                             meterreadingdatetime,
                             min(readingvalue) as readingvalue
                      from ref_readings_internal_valid
                      group by account_id,
                               meter_point_id,
                               register_id,
                               meterreadingdatetime),
     cte_first_read_date as (select account_id,
                                    meter_point_id,
                                    register_id,
                                    meterreadingdatetime,
                                    readingvalue,
                                    row_number()
                                    over (partition by account_id, meter_point_id, register_id order by meterreadingdatetime) as rn
                             from cte_readings),
     cte_latest_ind_eac as (select account_id,
                                   mpan,
                                   serial_number,
                                   register_id,
                                   estimation_value,
                                   effective_from,
                                   row_number()
                                   over (partition by account_id, mpan, serial_number, register_id order by effective_from desc) as rn
                            from ref_estimates_elec_internal),
     cte_mp_attributes as (select account_id,
                                  meter_point_id,
                                  attributes_attributename       as att_name,
                                  max(attributes_attributevalue) as att_value
                           from ref_meterpoints_attributes
                           where nvl(attributes_effectivefromdate, getdate()) <= getdate()
                             and nvl(attributes_effectivetodate, getdate()) >= getdate()
                           group by account_id, meter_point_id, attributes_attributename),
     cte_reg_attributes as (select account_id,
                                   meter_point_id,
                                   meter_id,
                                   register_id,
                                   registersattributes_attributename       as att_name,
                                   max(registersattributes_attributevalue) as att_value
                            from ref_registers_attributes
                            group by account_id, meter_point_id, meter_id, register_id,
                                     registersattributes_attributename)
select mp.account_id,
       mp.meter_point_id,
       met.meter_id,
       reg.register_id,
       mp.meterpointnumber,
       met.meterserialnumber                                     as meter_serial,
       rma_gsp.att_value                                         as gsp,
       rma_ssc.att_value                                         as ssc,
       reg.registers_tpr::bigint                                 as tpr,
       rma_pcl.att_value::int                                    as pcl,
       nvl(rra_nd.att_value::int, 5)                             as num_dials,
       2                                                         as smooth_param,
       first_read.meterreadingdatetime                           as first_read_date,
       first_read.readingvalue                                   as first_read_value,
       first_cppc.cumulative_ppc                                 as first_read_cppc,
       first_cppc.cumulative_ppc_count                           as first_read_cppc_count,
       nvl(nullif(iie.igl_ind_eac, 0), ind_eac.estimation_value) as prev_igl_ind_eac,
       prev_read.meterreadingdatetime                            as prev_read_date,
       prev_read.readingvalue                                    as prev_read_value,
       prev_cppc.cumulative_ppc                                  as prev_read_cppc,
       prev_cppc.cumulative_ppc_count                            as prev_read_cppc_count,
       today_cppc.cumulative_ppc                                 as today_cppc,
       today_cppc.cumulative_ppc_count                           as today_cppc_count,
       getdate()                                                 as etlchange
from ref_meterpoints mp
         inner join ref_meters met on mp.account_id = met.account_id and
                                      mp.meter_point_id = met.meter_point_id and
                                      met.removeddate is null
         inner join ref_registers reg on reg.account_id = met.account_id and
                                         reg.meter_point_id = met.meter_point_id and
                                         reg.meter_id = met.meter_id and
                                         reg.registers_tprperioddescription is not null
         left join cte_mp_attributes rma_gsp on rma_gsp.account_id = mp.account_id and
                                                rma_gsp.meter_point_id = mp.meter_point_id and
                                                rma_gsp.att_name = 'GSP'
         left join cte_mp_attributes rma_ssc on rma_ssc.account_id = mp.account_id and
                                                rma_ssc.meter_point_id = mp.meter_point_id and
                                                rma_ssc.att_name = 'SSC'
         left join cte_mp_attributes rma_pcl on rma_pcl.account_id = mp.account_id and
                                                rma_pcl.meter_point_id = mp.meter_point_id and
                                                rma_pcl.att_name = 'Profile Class'
         left join cte_reg_attributes rra_nd on rra_nd.account_id = reg.account_id and
                                                rra_nd.meter_point_id = reg.meter_point_id and
                                                rra_nd.meter_id = reg.meter_id and
                                                rra_nd.register_id = reg.register_id and
                                                rra_nd.att_name = 'No_Of_Digits'
         left join cte_latest_ind_eac ind_eac on ind_eac.account_id = mp.account_id and
                                                 ind_eac.mpan = mp.meterpointnumber and
                                                 ind_eac.serial_number = met.meterserialnumber and
                                                 ind_eac.register_id = reg.registers_registerreference and
                                                 ind_eac.rn = 1
         left join ref_calculated_igl_ind_eac iie on iie.account_id = mp.account_id and
                                                     iie.meterpoint_id = mp.meter_point_id and
                                                     iie.register_id = reg.register_id
         left join cte_first_read_date first_read on reg.account_id = first_read.account_id and
                                                     reg.meter_point_id = first_read.meter_point_id and
                                                     reg.register_id = first_read.register_id and
                                                     first_read.rn = 1
         left join cte_readings prev_read on reg.account_id = prev_read.account_id and
                                             reg.meter_point_id = prev_read.meter_point_id and
                                             reg.register_id = prev_read.register_id and
                                             trunc(nvl(iie.read_max_datetime_elec, ind_eac.effective_from)) =
                                             trunc(prev_read.meterreadingdatetime)
         left join ref_cumulative_ppc first_cppc
                   on trunc(first_cppc.ppc_date) = trunc(first_read.meterreadingdatetime) and
                      first_cppc.gsp = rma_gsp.att_value and
                      first_cppc.ssc = rma_ssc.att_value and
                      first_cppc.tpr = reg.registers_tpr::bigint and
                      first_cppc.pcl = rma_pcl.att_value
         left join ref_cumulative_ppc prev_cppc on trunc(prev_cppc.ppc_date) = trunc(prev_read.meterreadingdatetime) and
                                                   prev_cppc.gsp = rma_gsp.att_value and
                                                   prev_cppc.ssc = rma_ssc.att_value and
                                                   prev_cppc.tpr = reg.registers_tpr::bigint and
                                                   prev_cppc.pcl = rma_pcl.att_value
         left join ref_cumulative_ppc today_cppc on trunc(today_cppc.ppc_date) = trunc(getdate()) and
                                                    today_cppc.gsp = rma_gsp.att_value and
                                                    today_cppc.ssc = rma_ssc.att_value and
                                                    today_cppc.tpr = reg.registers_tpr::bigint and
                                                    today_cppc.pcl = rma_pcl.att_value
where nvl(least(mp.supplyenddate, mp.associationenddate), getdate() + 1) > getdate()
  and mp.meterpointtype = 'E'