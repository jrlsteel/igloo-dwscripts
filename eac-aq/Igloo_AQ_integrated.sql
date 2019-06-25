select
    account_id,
    LDZ as gas_ldz,
    gas_imperial_meter_indicator,
    meter_point_id as meterpoint_id,
    (select supplyenddate from ref_meterpoints rm
    where calc_params.account_id = rm.account_id
      and calc_params.meter_point_id = rm.meter_point_id) as supplyend_date,
    (select removeddate from ref_meters rm
    where rm.account_id = calc_params.account_id
      and rm.meter_point_id = calc_params.meter_point_id
      and rm.meter_id = calc_params.meter_id) as meter_removed_date,
    register_id,
    no_of_digits,
    open_date as read_min_datetime_gas,
    close_date as read_max_datetime_gas,
    open_val as read_min_readings_gas,
    close_val as read_max_readings_gas,
    datediff(months,open_date,close_date) as read_months_diff_gas,
    datediff(days,open_date,close_date) as read_days_diff_gas,
    meter_advance as read_consumption_gas,
    avg_cv as cv,
    cwaalp as waalp,
    meter_advance * 1.02264 * avg_cv * U * (1/3.6) as rmq,
    registers_eacaq as industry_aq_on_register,
    (select top 1 estimation_value from ref_estimates_gas_internal regi
        where calc_params.account_id = regi.account_id and calc_params.meterpointnumber = regi.mprn
        order by effective_from desc) as industry_aq_on_estimates,
    U as u,
    meter_advance * 1.02264 * avg_cv * U * (1/3.6) * 365 / cwaalp as igloo_aq_v1,
    getdate() as etlchange
from (
    select distinct
         read_pairs.account_id,
         read_pairs.register_id,
         rma_ldz.attributes_attributevalue                                          as LDZ,
         case when rma_imp.attributes_attributevalue='Y' then 2.83 else 1 end       as U,
         read_pairs.open_date,
         read_pairs.open_val,
         read_pairs.close_date,
         read_pairs.close_val,
         case when read_pairs.close_val < read_pairs.open_val --meter has rolled over
             then read_pairs.close_val - read_pairs.open_val + power(10,len(read_pairs.open_val))
             else read_pairs.close_val - read_pairs.open_val
         end                                                                     as meter_advance,
         (select sum((1 + ((waalp.value/2) * (waalp.variance))) * (waalp.forecastdocumentation)) --TODO: if/when the weather data is corrected, remove the /2
          from ref_alp_igloo_daf_wcf waalp
          where waalp.ldz = trim(rma_ldz.attributes_attributevalue)
            and waalp.applicable_for >= read_pairs.open_date
            and waalp.applicable_for < read_pairs.close_date)                       as cwaalp,
         (select avg(cv.value/2) --TODO: if/when the CV data is corrected, remove the /2
          from ref_alp_igloo_cv cv
          where cv.ldz = trim(rma_ldz.attributes_attributevalue)
            and cv.applicable_for >= read_pairs.open_date
            and cv.applicable_for < read_pairs.close_date)                          as avg_cv,

         -- additional info for the output table
         rma_imp.attributes_attributevalue                                          as gas_imperial_meter_indicator,
         reg_gas.meter_point_id,
         reg_gas.meter_id,
         reg_gas.registers_eacaq,
         read_pairs.no_of_digits,
         read_pairs.meterpointnumber
    from
        -- open / close read pairs
        (select
            *
        from
            (select
                --readings info
                read_close.account_id,
                read_close.register_id,
                read_close.meterreadingdatetime as close_date,
                read_close.readingvalue as close_val,
                read_open.meterreadingdatetime as open_date,
                read_open.readingvalue as open_val,
                read_close.no_of_digits, -- just for output table
                read_close.meterpointnumber,
                --info to inform selection of valid pairs (rows that contain the best opening reading for each closing reading)
                suitability_rank(datediff(days,read_open.meterreadingdatetime,read_close.meterreadingdatetime),2) as sr,
                min(suitability_rank(datediff(days,read_open.meterreadingdatetime,read_close.meterreadingdatetime),2))
                    over (partition by read_close.account_id, read_close.register_id, read_close.meterreadingdatetime) as best_sr
            from (select *
                  from (select *, row_number() over (partition by account_id, register_id order by meterreadingdatetime desc) as r
                        from temp_vw_ref_readings_all_valid where meterpointtype = 'G') ranked
                  where r = 1 ) read_close --TODO: this line should be replaced with "new reads" rather than calculating on every read
                inner join temp_vw_ref_readings_all_valid read_open
                    on read_open.register_id = read_close.register_id
                    and read_open.meterreadingsourceuid not in ('DCOPENING','DC')
                    --The following line can be removed to allow matching of register reads prior to account creation (NRL/NOSI)
                    --and read_open.account_id = read_close.account_id
                    and datediff(days,read_open.meterreadingdatetime,read_close.meterreadingdatetime) between 273 and (365*3)
            ) possible_read_pairs
        where sr = best_sr and open_date >= '2017-10-01' --TODO: adjust this open_date clause when we have more ALP data
        ) read_pairs
        --get meter_point_id from ref_registers
        left outer join ref_registers reg_gas
            on read_pairs.account_id = reg_gas.account_id and read_pairs.register_id = reg_gas.register_id --LDZ for the meterpoint
        left outer join ref_meterpoints_attributes rma_ldz
            on reg_gas.account_id = rma_ldz.account_id and
                reg_gas.meter_point_id = rma_ldz.meter_point_id and
                rma_ldz.attributes_attributename = 'LDZ' --Whether the meterpoint is Imperial or Metric
        left outer join ref_meterpoints_attributes rma_imp
            on reg_gas.account_id = rma_imp.account_id and
                reg_gas.meter_point_id = rma_imp.meter_point_id and
                rma_imp.attributes_attributename = 'Gas_Imperial_Meter_Indicator'
    ) calc_params;