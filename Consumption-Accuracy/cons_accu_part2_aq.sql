-- AQ view
                        select
                        acc_reg.account_id,
                        q.gas_usage as quotes_aq,
                        acc_reg.first_ind_aq,
                        round(case
                         when q.gas_usage > 0
                                 then 100 -
                                      (q.gas_usage / acc_reg.first_ind_aq * 100)
                         else NULL end ,
                         3) as quote_firstaq_var,

                         avg(datediff(days, acc_reg.current_aq_date, acc_reg.next_aq_date)) over (partition by acc_reg.account_id, acc_reg.register_id) as estimate_frequency,
                         datediff(days, acc_reg.latest_aq_date, getdate()) as estimate_age,
                         pa_cons_gas,
                         igl_ind_aq,
                         ind_aq,
                         quotes_aq

                              from -- get all registers for an account
                                   (select su.external_id as account_id,
                                           su.registration_id,
                                           mp.meterpointnumber,
                                           reg.register_id,
                                           mt.meterserialnumber,
                                           reg.registers_registerreference,
                                           mp.supplystartdate,
                                           max(case when greatest(mp.supplystartdate, mp.associationstartdate) = eg.effective_from
                                                     then estimation_value else 0 end) over (partition by su.external_id, eg.register_id) as first_ind_aq,
                                            -- for calculating frequency
                                           eg.effective_from current_aq_date,
                                           lead(eg.effective_from,1) OVER (PARTITION BY su.external_id, reg.register_id ORDER BY eg.effective_from) as next_aq_date,
                                           -- for calculating age
                                           max(eg.effective_from) over (partition by su.external_id, reg.register_id) latest_aq_date
                                    from ref_cdb_supply_contracts su
                                    inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G'
                                    inner join ref_meters mt on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and mt.removeddate is null
                                    inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
                                    left outer join ref_estimates_gas_internal eg on eg.account_id = su.external_id
                                                                               and mp.meterpointnumber = eg.mprn
                                                                               and reg.registers_registerreference = eg.register_id
                                                                               and mt.meterserialnumber = eg.serial_number
                              where su.external_id = 1833
                                    ) acc_reg
                        left outer join ref_cdb_registrations creg on creg.id = acc_reg.registration_id
                        left outer join ref_cdb_quotes q on q.id = creg.quote_id
                        left outer join ref_consumption_accuracy_gas cons_gas on cons_gas.account_id = acc_reg.account_id
                        -- inner join ref_readings_internal_valid ri on ri.account_id = su.external_id and ri.register_id = reg.register_id
                        -- inner join ref_readings_internal ri on ri.account_id = su.external_id and ri.register_id = reg.register_id


select * from ref_meterpoints where account_id = 1833;
select * from ref_estimates_gas_internal where account_id = 1833;
select * from ref_estimates_elec_internal where account_id = 1831;

select * from ref_meterpoints where account_id