drop view vw_ref_calculated_aq_v1_all;

create view vw_ref_calculated_aq_v1_all as
select st.account_id,
       st.gas_LDZ,
       st.gas_Imperial_Meter_Indicator,
       st.meterpoint_id,
       st.supplyend_date,
       st.meter_removed_date,
       st.register_id,
       st.no_of_digits,
       st.read_min_datetime_gas                                        as read_min_datetime_gas,
       st.read_max_datetime_gas                                        as read_max_datetime_gas,
       st.read_min_readings_gas                                        as read_min_readings_gas,
       st.read_max_readings_gas                                        as read_max_readings_gas,
       datediff(months, st.read_min_datetime_gas,
                st.read_max_datetime_gas)                              as read_months_diff_gas,
       datediff(days, st.read_min_datetime_gas,
                st.read_max_datetime_gas)                              as read_days_diff_gas,
       st.read_consumption_gas,
       st.cv as cv,
       st.waalp as waalp,
       ((st.read_consumption_gas * 1.02264 * st.cv * st.U) / 3.6)      as rmq,
       st.register_eac_gas                                             as industry_aq_on_register,
       st.latest_ind_aq_estimates                                      as industry_aq_on_estimates,
       st.U,
       round(calculate_aq_v1(coalesce(st.read_consumption_gas, 0), coalesce(st.cv, 0), coalesce(st.waalp, 0),
                       coalesce(st.gas_Imperial_Meter_Indicator, 'U'))::numeric, 2) as igloo_aq_v1,
       getdate()                                                       as etlchange
from (select mp_gas.account_id                                                                                                             as account_id,
             mp_gas.meter_point_id                                                                                                         as meterpoint_id,
             mp_gas.supplyenddate                                                                                                          as supplyend_date,
             mtrs_gas.removeddate                                                                                                          as meter_removed_date,
             reg_gas.register_id                                                                                                           as register_id,
             rma_ldz.attributes_attributevalue                                                                                             as gas_LDZ,
             rma_imp.attributes_attributevalue                                                                                             as gas_Imperial_Meter_Indicator,
             max(read_valid.no_of_digits)                                                                                                  as no_of_digits,
             max(reg_gas.registers_eacaq)                                                                                                  as register_eac_gas,
             min(read_valid.meterreadingdatetime)                                                                                          as read_min_datetime_gas,
             max(read_valid.meterreadingdatetime)                                                                                          as read_max_datetime_gas,
             min(read_valid.corrected_reading)                                                                                             as read_min_readings_gas,
             max(read_valid.corrected_reading)                                                                                             as read_max_readings_gas,
             max(read_valid.corrected_reading) - min(read_valid.corrected_reading)                                                         as read_consumption_gas,
             (select sum((1 + ((waalp.value/2) * (waalp.variance))) * (waalp.forecastdocumentation/2))
              from ref_alp_igloo_daf_wcf waalp
              where waalp.ldz = trim(rma_ldz.attributes_attributevalue)
                and waalp.date >= min(trunc(read_valid.meterreadingdatetime)) and waalp.date < max(trunc(read_valid.meterreadingdatetime)))        as waalp,
             (select 0.5 * avg(cv.value)
              from ref_alp_igloo_cv cv
              where cv.ldz = trim(rma_ldz.attributes_attributevalue)
                and cv.applicable_for between min(trunc(read_valid.meterreadingdatetime)) and max(trunc(read_valid.meterreadingdatetime))) as cv,
             case
               when rma_imp.attributes_attributevalue in ('N') then 1.00
               else case when rma_imp.attributes_attributevalue = 'Y' then 2.83 else 0 end end                                             as U,
             max(read_valid.latest_ind_aq)                                                                                                 as latest_ind_aq_estimates
      from ref_meterpoints mp_gas
             inner join ref_meterpoints_attributes rma_ldz on mp_gas.meter_point_id = rma_ldz.meter_point_id and
                                                              rma_ldz.attributes_attributename = 'LDZ'
             inner join ref_meterpoints_attributes rma_imp on mp_gas.meter_point_id = rma_imp.meter_point_id and
                                                              rma_imp.attributes_attributename in ('Gas_Imperial_Meter_Indicator')
             inner join ref_meters mtrs_gas on mtrs_gas.meter_point_id = mp_gas.meter_point_id and
                                               mtrs_gas.removeddate is NULL
             inner join ref_registers reg_gas on mtrs_gas.meter_id = reg_gas.meter_id
             left outer join (select y.account_id,
                                     y.meterpointnumber,
                                     y.meterpointtype,
                                     y.registerreference,
                                     y.register_id,
                                     y.no_of_digits,
                                     y.meterreadingdatetime,
                                     y.meterreadingcreateddate,
                                     y.corrected_reading,
                                     datediff(days, meterreadingdatetime, max(
                                                                            y.meterreadingdatetime) OVER (PARTITION BY y.account_id, y.register_id)) as days_diff,
                                     y.ind_aq,
                                     case when datediff(days, meterreadingdatetime,
                                                        max(y.meterreadingdatetime) OVER (PARTITION BY y.account_id, y.register_id)) = 0 then
                                                        y.ind_aq else 0 end as latest_ind_aq,
                                     min(meterreadingdatetime) over (partition by account_id, register_id) min_readings_datetime,
                                     y.row_num
                              from (select r.*,
                                           count(*) over (partition by r.account_id, r.register_id) as total_reads,
                                           dense_rank() over (partition by account_id, register_id order by meterreadingdatetime desc) row_num,
                                           coalesce((select top 1 estimation_value
                                                     from ref_estimates_gas_internal eg
                                                     where eg.account_id = r.account_id
                                                       and r.meterpointnumber = eg.mprn
                                                       and r.registerreference = eg.register_id
                                                       and r.meterserialnumber = eg.serial_number
                                                       and DATEDIFF(days, r.meterreadingdatetime, eg.effective_from) between 0 and 40
                                                     order by eg.effective_from desc), 0)                as ind_aq
                                    from vw_ref_readings_all_valid r
                                    where r.meterreadingdatetime > '2017-10-01'
                                   ) y
                              where y.total_reads >= 2
                                and y.ind_aq != 0
                                    ) read_valid
               on mp_gas.account_id = read_valid.account_id and reg_gas.register_id = read_valid.register_id and
                                            (
                                            (read_valid.days_diff = 0 or read_valid.days_diff between 273 and 365)
                                             )
                                     and read_valid.min_readings_datetime >'2017-10-01'
      where
             mp_gas.meterpointtype = 'G'
        and (mp_gas.supplyenddate is null or mp_gas.supplyenddate > getdate())
      group by mp_gas.account_id,
               mp_gas.meter_point_id,
               mp_gas.meterpointnumber,
               reg_gas.registers_registerreference,
               mtrs_gas.meterserialnumber,
               reg_gas.register_id,
               mp_gas.supplyenddate,
               mtrs_gas.removeddate,
               rma_ldz.attributes_attributevalue,
               rma_imp.attributes_attributevalue) st;