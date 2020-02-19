drop table if exists temp_estimated_advance_gas;
create table temp_estimated_advance_gas as
select x1.account_id,
       x1.register_id,
       x1.last_reading_date,
       x1.last_reading_value,
       x1.aq                                                        as igl_aq,
       x1.ind_aq,
       x1.effective_for,
       x1.igl_estimated_advance,
       x1.ind_estimated_advance,
       coalesce(rst_inner.tol_min * igl_estimated_advance / 100, 0) as igl_lower_threshold,
       coalesce(rst_inner.tol_max * igl_estimated_advance / 100, 0) as igl_higher_threshold,
       coalesce(rst_inner.tol_min * ind_estimated_advance / 100, 0) as ind_lower_threshold,
       coalesce(rst_inner.tol_max * ind_estimated_advance / 100, 0) as ind_inner_threshold,
       coalesce(rst_outer.tol_max * ind_estimated_advance / 100, 0) as ind_higher_threshold,
       coalesce(rra.registersattributes_attributevalue::int, 5)     as register_num_digits,
       getdate()                                                    as etlchange
from (select account_id,
             register_id,
             last_reading_date,
             last_reading_value,
             days_since_last_read,
             round(coalesce(aq, 0), 0)         as aq,
             round(coalesce(igl_ind_aq, 0), 0) as igl_ind_aq,
             round(coalesce(ind_aq, 0), 0)     as ind_aq,
             getdate()                         as effective_for,
             case
                 when last_reading_date is not null then
                     convert_kwh_to_cubic(aq * nvl(cwaalp, 0) / 365, avg_cv, imp_indicator)
                 end                           as igl_estimated_advance,
             case
                 when last_reading_date is not null then
                     convert_kwh_to_cubic(ind_aq * nvl(cwaalp, 0) / 365, avg_cv, imp_indicator)
                 end                           as ind_estimated_advance
      from (select mp.account_id                                                     as account_id,
                   r.register_id,
                   reads.meterreadingdatetime                                        as last_reading_date,
                   reads.readingvalue                                                as last_reading_value,
                   datediff(days, reads.meterreadingdatetime, trunc(getdate()))      as days_since_last_read,
                   coalesce(aq.igloo_aq, 0)                                          as igl_ind_aq,
                   coalesce(r.registers_eacaq, 0)                                    as ind_aq,
                   coalesce(nullif(aq.igloo_aq, 0), nullif(r.registers_eacaq, 0), 0) as aq,
                   rma_imp.attributes_attributevalue                                 as imp_indicator,
                   (select sum((1 + (nvl(waalp.value, 0) * 0.5 * waalp.variance)) * waalp.forecastdocumentation)
                           --TODO: if/when the weather data is corrected, remove the /2
                    from ref_alp_igloo_daf_wcf waalp
                    where waalp.ldz = trim(rma.attributes_attributevalue)
                      and waalp.date >= reads.meterreadingdatetime
                      and waalp.date < getdate() + 1)                                as cwaalp,
                   coalesce((select nvl(avg(cv.value / 2), 39.417)
                             from ref_alp_igloo_cv cv
                             where cv.ldz = trim(rma.attributes_attributevalue)
                               and cv.applicable_for >= reads.meterreadingdatetime
                               and cv.applicable_for < getdate() + 1), 0)            as avg_cv
            from ref_meterpoints mp
                     inner join ref_meters m
                                on m.account_id = mp.account_id and m.meter_point_id = mp.meter_point_id and
                                   m.removeddate is null
                     inner join ref_registers r on r.account_id = mp.account_id and r.meter_id = m.meter_id and
                                                   r.registers_tprperioddescription is not null
                     left join ref_meterpoints_attributes rma
                               on rma.account_id = mp.account_id and rma.meter_point_id = mp.meter_point_id and
                                  rma.attributes_attributename = 'LDZ'
                     left join ref_meterpoints_attributes rma_imp
                               on rma_imp.account_id = mp.account_id and
                                  rma_imp.meter_point_id = mp.meter_point_id and
                                  rma_imp.attributes_attributename = 'Gas_Imperial_Meter_Indicator'

                     left join (select *
                                from (select account_id,
                                             register_id,
                                             meterreadingdatetime,
                                             readingvalue,
                                             row_number()
                                             over (partition by ri.account_id, register_id order by meterreadingdatetime desc) rownum
                                      from temp_rriv ri
                                      order by ri.meterreadingdatetime desc) r1
                                where r1.rownum = 1) reads
                               on reads.account_id = mp.account_id and reads.register_id = r.register_id
                     left outer join ref_calculated_aq aq
                                     on aq.account_id = mp.account_id and aq.register_id = r.register_id
            where mp.meterpointtype = 'G'
              and (nvl(least(mp.supplyenddate, mp.associationenddate), getdate() + 1) >= getdate())) x
      order by x.account_id, register_id) x1
         left join ref_stg_tolerances rst_inner
                   on
                           rst_inner.tol_group = 'industry_tolerance_gas'
                           and rst_inner.group_id = 3
                           and rst_inner.lookup_key = 'ind_gas_inner'
                           and (rst_inner.effective_to is null or rst_inner.effective_to >= getdate())
                           and rst_inner.effective_from <= getdate()
                           and x1.ind_aq >= rst_inner.lookup_range_min
                           and x1.ind_aq <= rst_inner.lookup_range_max
         left join ref_stg_tolerances rst_outer
                   on rst_outer.tol_group = 'industry_tolerance_gas'
                       and rst_outer.group_id = 3
                       and rst_outer.lookup_key = 'ind_gas_outer'
                       and (rst_outer.effective_to is null or rst_outer.effective_to >= getdate())
                       and rst_outer.effective_from <= getdate()
                       and x1.ind_aq >= rst_outer.lookup_range_min
                       and x1.ind_aq <= rst_outer.lookup_range_max
         left join ref_registers_attributes rra
                   on rra.account_id = x1.account_id and rra.register_id = x1.register_id and
                      registersattributes_attributename = 'No_Of_Digits'
order by x1.account_id, x1.register_id
;