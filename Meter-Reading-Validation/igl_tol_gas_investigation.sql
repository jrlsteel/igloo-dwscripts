select x1.account_id,
       x1.register_id,
       x1.last_reading_date,
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
       getdate()                                                    as etlchange,

       aq                                                           as aq_prior,
       startdate                                                    as SSD,
       datediff(days, SSD, new_read_date)                           as days_since_ssd,
       prev_read_date                                               as read_date_1,
       new_read_date                                                as read_date_2,
       prev_read_val                                                as read_val_1,
       new_read_value                                               as read_val_2,
       read_val_2 - read_val_1                                      as actual_advance,
       actual_advance * 100 / igl_estimated_advance                 as perc
from (select startdate,
             x.prev_read_val,
             x.new_read_value,
             x.prev_read_date,
             x.new_read_date,
             account_id,
             register_id,
             last_reading_date,
             days_since_last_read,
             round(coalesce(aq, 0), 0)                                        as aq,
             round(coalesce(igl_ind_aq, 0), 0)                                as igl_ind_aq,
             round(coalesce(ind_aq, 0), 0)                                    as ind_aq,
             getdate()                                                        as effective_for,
             convert_kwh_to_cubic((aq * cwaalp) / 365, avg_cv, imp_indicator) as igl_estimated_advance,
             convert_kwh_to_cubic((ind_aq * (cast(days_since_last_read as double precision) / 365)), avg_cv,
                                  imp_indicator)                              as ind_estimated_advance
      from (select read_pairs.prev_read_val,
                   read_pairs.new_read_value,
                   read_pairs.prev_read_date,
                   read_pairs.new_read_date,
                   su.external_id                                                       as account_id,
                   r.register_id,
                   read_pairs.prev_read_date                                            as last_reading_date,
                   datediff(days, read_pairs.prev_read_date, read_pairs.new_read_date)  as days_since_last_read,
                   greatest(mp.supplystartdate, associationstartdate)                   as startdate,
                   least(mp.supplyenddate, associationenddate)                          as enddate,
                   aq_at_read                                                           as igl_ind_aq,
                   aq_at_read                                                           as ind_aq,
                   aq_at_read                                                           as aq,
                   rma_imp.attributes_attributevalue                                    as imp_indicator,
                   coalesce((select sum((1 + ((waalp.value / 2) * (waalp.variance))) *
                                        (waalp.forecastdocumentation)) --TODO: if/when the weather data is corrected, remove the /2
                             from ref_alp_igloo_daf_wcf waalp
                             where waalp.ldz = trim(rma.attributes_attributevalue)
                               and waalp.applicable_for >= read_pairs.prev_read_date
                               and waalp.applicable_for < read_pairs.new_read_date), 0) as cwaalp,
                   coalesce((select avg(cv.value / 2)
                             from ref_alp_igloo_cv cv
                             where cv.ldz = trim(rma.attributes_attributevalue)
                               and cv.applicable_for >= read_pairs.prev_read_date
                               and cv.applicable_for < read_pairs.new_read_date), 0)    as avg_cv
            from ref_cdb_supply_contracts su
                     inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G' and
                                                      (least(mp.supplyenddate, mp.associationenddate) is null or
                                                       least(mp.supplyenddate, mp.associationenddate) >= current_date)
                     left outer join ref_meterpoints_attributes rma
                                     on rma.account_id = su.external_id and rma.meter_point_id = mp.meter_point_id and
                                        rma.attributes_attributename = 'LDZ'
                     left outer join ref_meterpoints_attributes rma_imp
                                     on rma_imp.account_id = su.external_id and
                                        rma_imp.meter_point_id = mp.meter_point_id and
                                        rma_imp.attributes_attributename = 'Gas_Imperial_Meter_Indicator'
                     inner join ref_meters m
                                on m.account_id = su.external_id and m.meter_point_id = mp.meter_point_id and
                                   m.removeddate is null
                     inner join ref_registers r on r.account_id = su.external_id and r.meter_id = m.meter_id
                     inner join (select account_id,
                                        account_id                                                                as acid,
                                        register_id,
                                        trunc(meterreadingdatetime)                                               as new_read_date,
                                        readingvalue                                                              as new_read_value,
                                        lag(trunc(meterreadingdatetime))
                                        over (partition by account_id, register_id order by meterreadingdatetime) as prev_read_date,
                                        lag(readingvalue)
                                        over (partition by account_id, register_id order by meterreadingdatetime) as prev_read_val,
                                        (select top 1 estimation_value
                                         from ref_estimates_gas_internal_audit
                                         where account_id = acid
                                           and effective_from < new_read_date
                                         order by effective_from desc)                                            as aq_at_read
                                 from ref_readings_internal_valid
                                 where meterreadingsourceuid = 'CUSTOMER'
                                 order by account_id, register_id, new_read_date) read_pairs
                                on read_pairs.account_id = su.external_id and read_pairs.register_id = r.register_id and
                                   read_pairs.new_read_date != read_pairs.prev_read_date and
                                   read_pairs.new_read_value > read_pairs.prev_read_val
                     left outer join ref_calculated_aq aq
                                     on aq.account_id = su.external_id and aq.register_id = r.register_id) x
      order by x.account_id, register_id) x1
         left join ref_stg_tolerances rst_inner
                   on rst_inner.tol_group = 'industry_tolerance_gas'
                       and rst_inner.lookup_key = 'ind_gas_inner'
                       and (rst_inner.effective_to is null or rst_inner.effective_to >= getdate())
                       and rst_inner.effective_from <= getdate()
                       and x1.ind_aq >= rst_inner.lookup_range_min
                       and x1.ind_aq <= rst_inner.lookup_range_max
         left join ref_stg_tolerances rst_outer
                   on rst_outer.tol_group = 'industry_tolerance_gas'
                       and rst_outer.lookup_key = 'ind_gas_outer'
                       and (rst_outer.effective_to is null or rst_outer.effective_to >= getdate())
                       and rst_outer.effective_from <= getdate()
                       and x1.ind_aq >= rst_outer.lookup_range_min
                       and x1.ind_aq <= rst_outer.lookup_range_max
order by x1.account_id, x1.register_id;