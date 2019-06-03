drop view vw_ref_calculated_aq_all;

create view vw_ref_calculated_aq_all as
select st.account_id,
                       st.gas_LDZ,
                       st.gas_Imperial_Meter_Indicator,
                       st.meterpoint_id,
                       st.supplyend_date,
                       st.meter_removed_date,
                       st.register_id,
                       st.no_of_digits,
                       st.read_min_created_date_gas                               as read_min_created_date_gas,
                       st.read_max_created_date_gas                               as read_max_created_date_gas,
                       st.read_min_readings_gas                                   as read_min_readings_gas,
                       st.read_max_readings_gas                                   as read_max_readings_gas,
                       datediff(months, st.read_min_created_date_gas,
                                st.read_max_created_date_gas)                     as read_months_diff_gas,
                       datediff(days, st.read_min_created_date_gas,
                                st.read_max_created_date_gas)                     as read_days_diff_gas,
                       st.read_consumption_gas,
                       st.cv,
                       waalp,
                       ((st.read_consumption_gas * 1.02264 * st.cv * st.U) / 3.6) as rmq,
                       st.register_eac_gas                                        as industry_aq,
                       case
                         when waalp != 0 then (st.read_consumption_gas * 1.02264 * st.cv * st.U / 3.6) * 365 /
                                              waalp
                         else 0 end                                               as igloo_aq,
                       getdate() as etlchange
                from (select mp_gas.account_id                                                                                                         as account_id,
                             mp_gas.meter_point_id                                                                                                     as meterpoint_id,
                             mp_gas.supplyenddate                                                                                                      as supplyend_date,
                             mtrs_gas.removeddate                                                                                                      as meter_removed_date,
                             reg_gas.register_id                                                                                                       as register_id,
                             rma_ldz.attributes_attributevalue                                                                                         as gas_LDZ,
                             rma_imp.attributes_attributevalue                                                                                         as gas_Imperial_Meter_Indicator,
                             max(read_valid.no_of_digits)                    				                                                           as no_of_digits,
                             max(reg_gas.registers_eacaq)                                                                                              as register_eac_gas,
                             min(trunc(read_valid.meterreadingdatetime))                                                                               as read_min_created_date_gas,
                             max(trunc(read_valid.meterreadingdatetime))                                                                               as read_max_created_date_gas,
                             min(read_valid.corrected_reading)                                                                                         as read_min_readings_gas,
                             max(read_valid.corrected_reading)                                                                                         as read_max_readings_gas,
                             max(read_valid.corrected_reading) - min(read_valid.corrected_reading)                                                     as read_consumption_gas,
                             (select sum((1 + (waalp.value * waalp.variance)) * waalp.forecastdocumentation)
                              from ref_alp_igloo_daf_wcf waalp
                              where waalp.ldz = trim(rma_ldz.attributes_attributevalue)
                                and waalp.date between min(trunc(read_valid.meterreadingdatetime)) and max(trunc(read_valid.meterreadingdatetime)))        as waalp,
                             (select 0.5 * avg(cv.value)
                              from ref_alp_igloo_cv cv
                              where cv.ldz = trim(rma_ldz.attributes_attributevalue)
                                and cv.applicable_for between min(trunc(read_valid.meterreadingdatetime)) and max(trunc(read_valid.meterreadingdatetime))) as cv,
                             case
                               when rma_imp.attributes_attributevalue in ('N') then 1.00
                               else 2.83 end                                                                                                           as U
                      from ref_meterpoints mp_gas
                             inner join ref_meterpoints_attributes rma_ldz on mp_gas.account_id = rma_ldz.account_id and mp_gas.meter_point_id = rma_ldz.meter_point_id and
                                                                              rma_ldz.attributes_attributename = 'LDZ'
                             inner join ref_meterpoints_attributes rma_imp on mp_gas.account_id = rma_imp.account_id and mp_gas.meter_point_id = rma_imp.meter_point_id and
                                                                              rma_imp.attributes_attributename = 'Gas_Imperial_Meter_Indicator'
                             inner join ref_meters mtrs_gas on mp_gas.account_id = mtrs_gas.account_id and mtrs_gas.meter_point_id = mp_gas.meter_point_id and
                                                               mtrs_gas.removeddate is NULL
                             inner join ref_registers reg_gas on mp_gas.account_id = reg_gas.account_id and mtrs_gas.meter_id = reg_gas.meter_id
                             inner join vw_ref_readings_all_valid read_valid
                                             on mp_gas.account_id = read_valid.account_id and reg_gas.register_id = read_valid.register_id and read_valid.meterreadingdatetime >= '2017-10-01'
                      where
                        mp_gas.meterpointtype = 'G'
                        and (mp_gas.supplyenddate is null or mp_gas.supplyenddate > getdate())
                      group by mp_gas.account_id,
                               mp_gas.meter_point_id,
                               reg_gas.register_id,
                               mp_gas.supplyenddate,
                               mtrs_gas.removeddate,
                               rma_ldz.attributes_attributevalue,
                               rma_imp.attributes_attributevalue) st;