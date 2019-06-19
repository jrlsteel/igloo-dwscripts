-- AQ view
create view vw_ref_customer_metrics_aq (
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
)

select * from ref_meterpoints where account_id = 1833;
select * from ref_estimates_gas_internal where account_id = 1833;
select * from ref_estimates_elec_internal where account_id = 1831;

select * from ref_meterpoints where account_id

select account_id,
       first_ind_aq,
       quote_firstaq_var,
       estimate_frequency,
       estimate_age,
       pa_cons_gas,
       igl_ind_aq,
       ind_aq,
       quotes_aq
from (select acc_reg.account_id,
             acc_reg.first_ind_aq,
          -- SM creating the variance between quote gas usage and the very 1st registers aq not sure this is right.
             round(case
                     when q.gas_usage > 0 and acc_reg.first_ind_aq > 0
                             then 100 -
                                  (q.gas_usage / acc_reg.first_ind_aq * 100)
                     else NULL end,
                   3) as                                                                                                                     quote_firstaq_var,
          -- SM taking the the date diff between between aq's for each register and then averaging
             avg(
               datediff(days, acc_reg.current_aq_date, acc_reg.next_aq_date)) over (partition by acc_reg.account_id, acc_reg.register_id) as estimate_frequency,
          -- SM taking the date diff from the latest aq for all registers
             datediff(days, acc_reg.latest_aq_date, getdate()) as                                                                            estimate_age,
             pa_cons_gas,
             igl_ind_aq,
             ind_aq,
             quotes_aq

      from -- get all registers for an account
           (select su.external_id                                                                         as account_id,
                   su.registration_id,
                   mp.meterpointnumber,
                   reg.register_id,
                   mt.meterserialnumber,
                   reg.registers_registerreference,
                   mp.supplystartdate,
                   max(case
                         when greatest(mp.supplystartdate, mp.associationstartdate) = eg.effective_from
                                 then estimation_value
                         else 0 end) over (partition by su.external_id, eg.register_id)                   as first_ind_aq,
                -- for calculating frequency
                   eg.effective_from                                                                         current_aq_date,
                   lead(eg.effective_from,
                        1) OVER (PARTITION BY su.external_id, reg.register_id ORDER BY eg.effective_from) as next_aq_date,
                -- for calculating age
                   max(eg.effective_from) over (partition by su.external_id, reg.register_id)                latest_aq_date
            from ref_cdb_supply_contracts su
                   inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G'
                   inner join ref_meters mt
                     on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and
                        mt.removeddate is null
                   inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
                   left outer join ref_estimates_gas_internal eg on eg.account_id = su.external_id
                                                                      and mp.meterpointnumber = eg.mprn
                                                                      and
                                                                    reg.registers_registerreference = eg.register_id
                                                                      and mt.meterserialnumber = eg.serial_number
           -- where su.external_id = 27400
           ) acc_reg
             left outer join ref_cdb_registrations creg on creg.id = acc_reg.registration_id
             left outer join ref_cdb_quotes q on q.id = creg.quote_id
             left outer join ref_consumption_accuracy_gas cons_gas on cons_gas.account_id = acc_reg.account_id)
group by account_id,
         first_ind_aq,
         quote_firstaq_var,
         estimate_frequency,
         estimate_age,
         pa_cons_gas,
         igl_ind_aq,
         ind_aq,
         quotes_aq

select * from  ref_estimates_gas_internal
where account_id =1833;

 select mp.account_id,reg.nocount
 from ref_meterpoints mp
        inner join (select ref_reg.account_id, count(*) as nocount
                    from ref_registers ref_reg
                    group by account_id
                    having count(*) > 3
                    order by ref_reg.account_id desc) reg on mp.account_id = reg.account_id and mp.meterpointtype = 'G'
where mp.meterpointtype ='G'

select ref_reg.account_id,ref_reg.meter_point_id, count(*) as nocount
from ref_registers ref_reg
inner join ref_meterpoints mp on mp.meter_point_id = ref_reg.meter_point_id and mp.meterpointtype ='G'
group by ref_reg.account_id,ref_reg.meter_point_id
having count(*) > 3
order by ref_reg.account_id desc

select mp.account_id, mp.meter_point_id, mt.meter_id, count(*)
from ref_meterpoints mp
       inner join ref_meters mt on mp.account_id = mt.account_id and mp.meter_point_id = mt.meter_point_id
       inner join ref_registers reg
         on mt.account_id = reg.account_id and mt.meter_point_id = reg.meter_point_id and mt.meter_id = reg.meter_id
where mp.meterpointtype = 'G'
and mt.removeddate is null
group by mp.account_id, mp.meter_point_id, mt.meter_id
having count(*) > 1

select * from ref_registers
where account_id = 27400

select * from ref_meterpoints
where account_id = 27400
