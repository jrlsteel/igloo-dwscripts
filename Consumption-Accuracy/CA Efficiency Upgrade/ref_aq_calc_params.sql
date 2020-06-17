-- truncate table ref_aq_calc_params;
-- insert into ref_aq_calc_params
with cte_ordered_readings as (select account_id,
                                     meter_point_id,
                                     meter_id,
                                     register_id,
                                     meterreadingdatetime,
                                     readingvalue,
                                     row_number()
                                     over (partition by account_id, meter_point_id, meter_id, register_id
                                         order by meterreadingdatetime) as rn
                              from vw_readings_aq_all),
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
       met.meterserialnumber                                   as meter_serial,
       rma_ldz.att_value                                       as ldz,
       nvl(rra_nd.att_value::int, 5)                           as num_dials,
       nullif(rma_imp.att_value, 'U') = 'Y'                    as imperial,
       frd.meterreadingdatetime                                as first_read_date,
       frd.readingvalue                                        as first_read_value,
       nvl(iia.igl_ind_aq, 0)                                  as prev_igl_ind_aq,
       nvl(iia.read_max_datetime_gas, '2000-01-01'::timestamp) as prev_read_date,
       cwaalp.cwaalp                                           as today_cwaalp,
       cwaalp.waalp_count                                      as today_waalp_count,
       cwaalp.ccv                                              as today_ccv,
       cwaalp.cv_count                                         as today_cv_count,
       getdate()                                               as etlchange
from ref_meterpoints mp
         inner join ref_meters met on mp.account_id = met.account_id and
                                      mp.meter_point_id = met.meter_point_id and
                                      met.removeddate is null
         inner join ref_registers reg on reg.account_id = met.account_id and
                                         reg.meter_point_id = met.meter_point_id and
                                         reg.meter_id = met.meter_id and
                                         reg.registers_tprperioddescription is not null
         left join cte_mp_attributes rma_ldz on rma_ldz.account_id = mp.account_id and
                                                rma_ldz.meter_point_id = mp.meter_point_id and
                                                rma_ldz.att_name = 'LDZ'
         left join cte_mp_attributes rma_imp on rma_imp.account_id = mp.account_id and
                                                rma_imp.meter_point_id = mp.meter_point_id and
                                                rma_imp.att_name = 'Gas_Imperial_Meter_Indicator'
         left join cte_reg_attributes rra_nd on rra_nd.account_id = reg.account_id and
                                                rra_nd.meter_point_id = reg.meter_point_id and
                                                rra_nd.meter_id = reg.meter_id and
                                                rra_nd.register_id = reg.register_id and
                                                rra_nd.att_name = 'No_Of_Digits'
         left join ref_calculated_igl_ind_aq iia on iia.account_id = mp.account_id and
                                                    iia.meterpoint_id = mp.meter_point_id and
                                                    iia.register_id = reg.register_id
         left join cte_ordered_readings frd on frd.account_id = reg.account_id and
                                               frd.meter_point_id = reg.meter_point_id and
                                               frd.meter_id = reg.meter_id and
                                               frd.register_id = reg.register_id and
                                               frd.rn = 1
         left join ref_cumulative_alp_cv cwaalp on cwaalp.ldz = rma_ldz.att_value and
                                                   trunc(cwaalp.coeff_date) = trunc(getdate())
where nvl(least(mp.supplyenddate, mp.associationenddate), getdate() + 1) > getdate()
  and mp.meterpointtype = 'G'
order by account_id, meter_point_id, meter_id, register_id
