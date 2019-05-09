-- delete
-- from ref_calculated_aq_v1;
-- drop table temp_ref_calculated_aq;
-- create table ref_calculated_aq_v1 as
-- insert into ref_calculated_aq_v1
-- AQ V1 batch--
select * from (
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
      case
         when st.waalp != 0 and st.cv != 0 and st.read_consumption_gas != 0 then
           (((st.read_consumption_gas * 1.02264 * st.cv * st.U) / 3.6) * 365) /
           st.waalp
         else 0 end                                                    as igloo_aq_v1,
       round(calculate_aq_v1(coalesce(st.read_consumption_gas, 0), coalesce(st.cv, 0), coalesce(st.waalp, 0),
                       coalesce(st.gas_Imperial_Meter_Indicator, 'U'))::numeric, 2) as udf_igloo_aq_v1,

       st.register_eac_gas - calculate_aq_v1(coalesce(st.read_consumption_gas, 0), coalesce(st.cv, 0), coalesce(st.waalp, 0),
                       coalesce(st.gas_Imperial_Meter_Indicator, 'U')) as ind_minus_igloo_aq,
--        st.latest_ind_aq_estimates - (case
--                                        when st.waalp != 0 then
--            (st.read_consumption_gas * 1.02264 * st.cv * st.U / 3.6) * 365 /
--            st.waalp
--                                        else 0 end)                as ind_minus_igloo_aq,
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
             min(trunc(read_valid.meterreadingdatetime))                                                                                   as read_min_datetime_gas,
             max(trunc(read_valid.meterreadingdatetime))                                                                                   as read_max_datetime_gas,
             min(read_valid.corrected_reading)                                                                                             as read_min_readings_gas,
             max(read_valid.corrected_reading)                                                                                             as read_max_readings_gas,
             max(read_valid.corrected_reading) - min(read_valid.corrected_reading)                                                         as read_consumption_gas,
             (select sum((1 + (waalp.value * waalp.variance)) * waalp.forecastdocumentation)
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
                                    from ref_readings_internal_valid r
                                    where r.meterreadingdatetime > '2017-10-01'
                                   ) y
                              where y.total_reads >= 2
                                and y.ind_aq != 0
                                    ) read_valid
               on mp_gas.account_id = read_valid.account_id and reg_gas.register_id = read_valid.register_id and
                                            (
                                            (read_valid.days_diff = 0 or read_valid.days_diff between 273 and 365)
--                                                 or read_valid.row_num = 2
                                             )

--                                             (read_valid.days_diff = 184  or read_valid.days_diff = 0)
                                     and read_valid.min_readings_datetime >'2017-10-01'
      where
--             mp_gas.account_id in (5773) and
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
               rma_imp.attributes_attributevalue) st
    ) st1
where
    st1.ind_minus_igloo_aq between -1 and 1 and
      st1.industry_aq_on_register!=0 and st1.udf_igloo_aq_v1!=0
;

effective_from,effective_to,estimation_value
2019-03-25 15:13:00.773000,9999-12-31 23:59:59.000000,24639
2019-02-21 19:12:43.443000,9999-12-31 23:59:59.000000,24824
2019-01-28 08:54:57.207000,9999-12-31 23:59:59.000000,24591
2018-12-20 17:13:44.753000,9999-12-31 23:59:59.000000,24461
2018-11-24 18:11:59.617000,9999-12-31 23:59:59.000000,24239
2018-10-26 11:12:17.977000,9999-12-31 23:59:59.000000,24019
2018-09-24 20:11:43.600000,9999-12-31 23:59:59.000000,24517
2018-08-26 12:11:42.883000,9999-12-31 23:59:59.000000,24744
2018-08-13 16:49:51.430000,9999-12-31 23:59:59.000000,24848
2018-06-22 17:12:17.863000,9999-12-31 23:59:59.000000,24395
2018-05-29 16:11:11.803000,9999-12-31 23:59:59.000000,24934
2018-02-01 00:00:00.000000,9999-12-31 23:59:59.000000,26109


select y.account_id,
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
                                    from ref_readings_internal_valid r
                                    where r.meterreadingdatetime > '2017-10-01'
                                   ) y
                              where y.total_reads >= 2
                              and y.meterpointtype = 'G'
                                and y.ind_aq != 0
                                    and y.account_id = 6942



select distinct attributes_attributevalue
from ref_meterpoints_attributes
where attributes_attributename = 'Gas_Imperial_Meter_Indicator';


create or replace function calculate_aq_v1(consumption double precision, calorific_value double precision,
                                           cwaalp      double precision, imperial_meter_indicator character(1))
  returns double precision
  stable
  language plpythonu
as $$
  import logging
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)

  aq = 0.0
  U = 0.0
  if imperial_meter_indicator == 'N':
    U = 1.00
  if imperial_meter_indicator == 'Y':
    U = 2.83
  cv = calorific_value
  cf = 1.02264
  cnf = 3.6
  rmq = (consumption * cv * cf * U) / cnf
  if cwaalp == 0:
    aq = 0
  else:
    aq = (rmq * 365) / cwaalp
  return aq
$$;


select *
from svl_udf_log;

select *
from ref_estimates_gas_internal
where account_id = 5759;

-- between -1 and 1 = 1
-- between -1 and 1 = 1
-- between -1 and 1 = 1

select y.account_id,
       y.meterpointnumber,
       y.meterpointtype,
       y.registerreference,
       y.register_id,
       y.no_of_digits,
       y.meterreadingdatetime,
       y.meterreadingcreateddate,
       y.corrected_reading,
       datediff(days, meterreadingdatetime,
                max(y.meterreadingdatetime) OVER (PARTITION BY y.account_id, y.register_id)) as days_diff,
       y.ind_aq,
       case when datediff(days, meterreadingdatetime,
                max(y.meterreadingdatetime) OVER (PARTITION BY y.account_id, y.register_id)) = 0 then
                y.latest_ind_aq else 0 end as latest_ind_aq,
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
      from temp_ref_readings_internal_valid r
      where r.meterreadingdatetime > '2017-10-01'
     ) y
where y.account_id = 5773
  and y.total_reads >= 2
  and y.meterpointtype = 'G'
--   and y.latest_ind_aq != 0
    --   and ((y.days_diff > 270 and y.days_diff <= 2 * 365) or y.days_diff = 0)
    --   and y.register_id = 1980
order by y.account_id, y.register_id, meterreadingdatetime desc;

select * from (
select r.*, min(meterreadingdatetime) over (partition by account_id, register_id) min_readings_datetime  from ref_readings_internal_valid r
) y
where y.min_readings_datetime > '2017-10-01' and y.meterpointtype = 'G' and account_id = 1870;

select * from temp_ref_readings_internal_valid where account_id = 6304

select eg.*,
       dateadd(month, datediff(month,0,getdate())-1,0),
       dense_rank() over (partition by account_id, mprn, register_id, serial_number order by effective_from desc)
from ref_estimates_gas_internal eg;

select add_months(last_day(getdate()), -2) + 10 ;

select * from ref_meterpoints where meter_point_id = 2101;
select * from ref_meterpoints where meterpointnumber = 2000007809594;

select status, count(*) from ref_account_status group by status
select status from ref_registrations_meterpoints_status_elec group by status;
