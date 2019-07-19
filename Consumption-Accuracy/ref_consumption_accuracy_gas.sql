select cons_acc_gas_old.*, cons_acc_gas_new.*
from (
         select acc_aggs.account_id               as account_id,
                latest_readings.max_read_datetime as reading_datetime,
                nvl(acc_aggs.pa_cons_gas, 0)      as pa_cons_gas,
                nvl(acc_aggs.igl_ind_aq, 0)       as igl_ind_aq,
                nvl(acc_aggs.ind_aq, 0)           as ind_aq,
                nvl(case
                        when q.gas_usage is null then
                            (q.gas_projected - (3.65 * q.gas_standing)) / (q.gas_unit / 100)
                        else q.gas_usage end, 0)  as quotes_aq,
                current_timestamp                 as etlchange
         from (select account_id,
                      registration_id,
                      case
                          when count(reg_level.pa_cons_gas) < count(*) then 0
                          else sum(reg_level.pa_cons_gas) end as pa_cons_gas,
                      case
                          when count(reg_level.igl_ind_aq) < count(*) then 0
                          else sum(reg_level.igl_ind_aq) end  as igl_ind_aq,
                      sum(reg_level.ind_aq)                   as ind_aq
               from (select su.external_id          as account_id,
                            su.registration_id,
                            pa_aq.igloo_aq          as pa_cons_gas,
                            calc_aq.igl_ind_aq      as igl_ind_aq,
                            ind_aq.estimation_value as ind_aq
                     from ref_cdb_supply_contracts su
                              inner join ref_meterpoints mp
                                         on mp.account_id = su.external_id and mp.meterpointtype = 'G'
                              inner join ref_meters mt
                                         on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and
                                            mt.removeddate is null
                              inner join ref_registers reg
                                         on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
                              left join ref_calculated_aq pa_aq
                                        on su.external_id = pa_aq.account_id and
                                           reg.register_id = pa_aq.register_id
                              left join ref_calculated_igl_ind_aq calc_aq
                                        on su.external_id = calc_aq.account_id and
                                           reg.register_id = calc_aq.register_id
                              left join (select *,
                                                row_number()
                                                over (partition by account_id,
                                                    mprn,
                                                    register_id,
                                                    serial_number
                                                    order by effective_from desc) as rn
                                         from ref_estimates_gas_internal) ind_aq
                                        on ind_aq.rn = 1 and
                                           su.external_id = ind_aq.account_id and
                                           mp.meterpointnumber = ind_aq.mprn and
                                           reg.registers_registerreference = ind_aq.register_id and
                                           mt.meterserialnumber = ind_aq.serial_number
                    ) reg_level
               group by account_id, registration_id
              ) acc_aggs
                  left join (select account_id, max(meterreadingdatetime) as max_read_datetime
                             from ref_readings_internal_valid
                             group by account_id) latest_readings
                            on latest_readings.account_id = acc_aggs.account_id
                  left join ref_cdb_registrations creg on acc_aggs.registration_id = creg.id
                  left join ref_cdb_quotes q on q.id = creg.quote_id
     ) cons_acc_gas_new
         full outer join ref_consumption_accuracy_gas cons_acc_gas_old on
    cons_acc_gas_new.account_id = cons_acc_gas_old.account_id
;
/*case
        when count(reg_level.igl_ind_aq) < count(reg_level.register_id) then 0
        else sum(reg_level.igl_ind_aq) end                           as igl_ind_aq_acc,*/
