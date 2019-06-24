-- drop view vw_ref_consumption_accuracy_elec;
--
-- select count(*)
-- from vw_ref_consumption_accuracy_elec
-- where best_consumption is null
--
-- select count(*)
-- from vw_ref_consumption_accuracy_elec
-- where best_consumption is not null;
--
-- select * from ref_consumption_accuracy_elec
-- order by account_id
--
-- create or replace view vw_ref_consumption_accuracy_elec as
--
--   select con_elec.account_id,
--          con_elec.reading_datetime,
--          rest.ind_firsteac_read_based_acc,
--          rest.quote_firsteac_var,
--          rest.ind_estimate_frequency  as estimate_frequency,
--          rest.igloo_estimate_age      as estimate_age,
--          con_elec.pa_cons_elec,
--          con_elec.igl_ind_eac,
--          con_elec.ind_eac,
--          con_elec.quotes_eac,
--          get_best_consumption(con_elec.igl_ind_eac, con_elec.ind_eac, con_elec.pa_cons_elec, con_elec.quotes_eac,
--                               'elec') as best_consumption,
--          con_elec.etlchange
--   from   ref_consumption_accuracy_elec con_elec
--   left outer join
--        (select x.account_id                                                                                                                               as account_id,
--                max(x.supplystartdate)                                                                                                                     as supplystartdate,
--                max(x.supplyenddate)                                                                                                                       as supplyenddate,
--                max(x.associationstartdate)                                                                                                                as assocstartdate,
--                max(x.associationenddate)                                                                                                                  as assocenddate,
--                x.latest_reading_datetime                                                                                                                  as reading_datetime,
--                datediff(day, max(x.supplystartdate),
--                         nvl(max(x.supplyenddate), getdate()))                                                                                             as mp_supply_age,
--                datediff(day, max(x.associationstartdate),
--                         nvl(max(x.associationenddate), getdate()))                                                                                        as mp_assoc_supply_age,
--                (select count(*)
--                 from ref_estimates_elec_internal rei
--                 where rei.account_id = x.account_id
--                   and rei.effective_from >= max(x.supplystartdate))                                                                                       as ind_estimate_count,
--                max(ind_firsteac_acc)                                                                                                                      as ind_firsteac_read_based_acc,
--                round(case
--                        when max(ind_firsteac_acc) > 0
--                                then 100 -
--                                     (cast(max(x.quotes_eac_acc) as double precision) / max(ind_firsteac_acc) * 100)
--                        else NULL end,
--                      3)                                                                                                                                      quote_firsteac_var,
--                case
--                  when (select count(*)
--                        from ref_estimates_elec_internal rei
--                        where rei.account_id = x.account_id
--                          and rei.effective_from >= max(x.supplystartdate)) > 0 then
--                    datediff(day, max(x.supplystartdate), nvl(max(x.supplyenddate), getdate())) / (select count(*)
--                                                                                                   from ref_estimates_elec_internal rei
--                                                                                                   where rei.account_id = x.account_id
--                                                                                                     and rei.effective_from >= max(x.supplystartdate)) end as ind_estimate_frequency,
--                datediff(day, x.latest_reading_datetime, getdate())                                                                                        as igloo_estimate_age,
--                cast(max(x.quotes_eac_acc) as double precision)                                                                                            as quotes_eac,
--                getdate()                                                                                                                                  as etlchange
--         from (select reads.external_id                                                                             as account_id,
--                      reads.supplystartdate,
--                      reads.supplyenddate,
--                      reads.associationstartdate,
--                      reads.associationenddate,
--                      reads.register_id                                                                             as register_id,
--                      reads.register_reading_id                                                                        reading_id,
--                      reads.meterreadingdatetime                                                                    as reading_datetime,
--                      reads.latest_reading_datetime,
--                      reads.latest_read_per_register,
--                      ee.estimation_value                                                                           as ind_eac_reg,
--                      q.electricity_usage                                                                              quotes_eac,
--                      sum(
--                        ee.estimation_value) over (partition by reads.external_id, reads.latest_read_per_register)  as ind_eac_acc,
--                      max(
--                        q.electricity_usage) over (partition by reads.external_id, reads.latest_read_per_register)  as quotes_eac_acc,
--                      sum(
--                        ee1.estimation_value) over (partition by reads.external_id, reads.latest_read_per_register) as ind_firsteac_acc
--             --- Getting
--               from (select su.external_id,
--                            mp.supplystartdate,
--                            mp.supplyenddate,
--                            mp.associationstartdate,
--                            mp.associationenddate,
--                            su.registration_id,
--                            mp.meterpointnumber,
--                            reg.register_id,
--                            ri.register_reading_id,
--                            mt.meterserialnumber,
--                            reg.registers_eacaq,
--                            reg.registers_registerreference,
--                            ri.meterreadingdatetime,
--                            max(ri.meterreadingdatetime) over (partition by su.external_id)                                        as latest_reading_datetime,
--                            min(ri.meterreadingdatetime) over (partition by su.external_id)                                        as earliest_reading_datetime,
--                            row_number() over (partition by su.external_id, reg.register_id order by ri.meterreadingdatetime desc) as latest_read_per_register
--                     from ref_cdb_supply_contracts su
--                            inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'E'
--                            inner join ref_meters mt
--                              on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and
--                                 mt.removeddate is null
--                            inner join ref_registers reg
--                              on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
--                            inner join ref_readings_internal_valid ri
--                              on ri.account_id = su.external_id and ri.register_id = reg.register_id
--                     --where mp.account_id =1831
--                    ) reads
--                      left outer join ref_estimates_elec_internal ee
--                        on ee.account_id = reads.external_id and ee.mpan = reads.meterpointnumber and
--                           ee.register_id = reads.registers_registerreference and
--                           ee.serial_number = reads.meterserialnumber and
--                           ee.effective_from = reads.meterreadingdatetime
--                      left outer join ref_estimates_elec_internal ee1
--                        on ee1.account_id = reads.external_id and ee1.mpan = reads.meterpointnumber and
--                           --   ee1.register_id = reads.registers_registerreference and
--                           --    ee1.serial_number = reads.meterserialnumber and
--                           ee1.effective_from = greatest(reads.associationstartdate, reads.supplystartdate)
--                      left outer join ref_cdb_registrations creg on creg.id = reads.registration_id
--                      left outer join ref_cdb_quotes q on q.id = creg.quote_id) x
--         where x.latest_read_per_register = 1
--       -- and x.account_id in (1831, 23983, 1893,1854)
--         group by x.account_id, x.latest_reading_datetime
--         order by x.account_id, x.latest_reading_datetime) rest
--       on rest.account_id = con_elec.account_id
--   where con_elec.account_id =9731
-- order by account_id;
--
--
-- select * from ref_estimates_elec_internal
-- where account_id = 1831
--
-- select * from ref_meterpoints
-- where account_id = 1831
--
--  from ref_meterpoints
-- where account_id = 1831
--
-- select * from ref_estimates_elec_internal
-- where account_id = 9731
--
--
-- select * from ref_meterpoints
-- where account_id = 9731
--
-- select * from ref_registers
-- where account_id = 9731
--
--
--
-- select x.account_id                                                                                                                               as account_id,
--                max(x.supplystartdate)                                                                                                                     as supplystartdate,
--                max(x.supplyenddate)                                                                                                                       as supplyenddate,
--                max(x.associationstartdate)                                                                                                                as assocstartdate,
--                max(x.associationenddate)                                                                                                                  as assocenddate,
--                x.latest_reading_datetime                                                                                                                  as reading_datetime,
--                datediff(day, max(x.supplystartdate),
--                         nvl(max(x.supplyenddate), getdate()))                                                                                             as mp_supply_age,
--                datediff(day, max(x.associationstartdate),
--                         nvl(max(x.associationenddate), getdate()))                                                                                        as mp_assoc_supply_age,
--                (select count(*)
--                 from ref_estimates_elec_internal rei
--                 where rei.account_id = x.account_id
--                   and rei.effective_from >= max(x.supplystartdate))                                                                                       as ind_estimate_count,
--                max(ind_firsteac_acc)                                                                                                                      as ind_firsteac_read_based_acc,
--                round(case
--                        when max(ind_firsteac_acc) > 0
--                                then 100 -
--                                     (cast(max(x.quotes_eac_acc) as double precision) / max(ind_firsteac_acc) * 100)
--                        else NULL end,
--                      3)                                                                                                                                      quote_firsteac_var,
--                case
--                  when (select count(*)
--                        from ref_estimates_elec_internal rei
--                        where rei.account_id = x.account_id
--                          and rei.effective_from >= max(x.supplystartdate)) > 0 then
--                    datediff(day, max(x.supplystartdate), nvl(max(x.supplyenddate), getdate())) / (select count(*)
--                                                                                                   from ref_estimates_elec_internal rei
--                                                                                                   where rei.account_id = x.account_id
--                                                                                                     and rei.effective_from >= max(x.supplystartdate)) end as ind_estimate_frequency,
--                datediff(day, x.latest_reading_datetime, getdate())                                                                                        as igloo_estimate_age,
--                cast(max(x.quotes_eac_acc) as double precision)                                                                                            as quotes_eac,
--                getdate()                                                                                                                                  as etlchange
--         from (select reads.external_id                                                                             as account_id,
--                      reads.supplystartdate,
--                      reads.supplyenddate,
--                      reads.associationstartdate,
--                      reads.associationenddate,
--                      reads.register_id                                                                             as register_id,
--                      reads.register_reading_id                                                                        reading_id,
--                      reads.meterreadingdatetime                                                                    as reading_datetime,
--                      reads.latest_reading_datetime,
--                      reads.latest_read_per_register,
--                      ee.estimation_value                                                                           as ind_eac_reg,
--                      q.electricity_usage                                                                              quotes_eac,
--                      sum(
--                        ee.estimation_value) over (partition by reads.external_id, reads.latest_read_per_register)  as ind_eac_acc,
--                      max(
--                        q.electricity_usage) over (partition by reads.external_id, reads.latest_read_per_register)  as quotes_eac_acc,
--                      sum(
--                        ee1.estimation_value) over (partition by reads.external_id, reads.latest_read_per_register) as ind_firsteac_acc
--             --- Getting
--               from (select su.external_id,
--                            mp.supplystartdate,
--                            mp.supplyenddate,
--                            mp.associationstartdate,
--                            mp.associationenddate,
--                            su.registration_id,
--                            mp.meterpointnumber,
--                            reg.register_id,
--                            ri.register_reading_id,
--                            mt.meterserialnumber,
--                            reg.registers_eacaq,
--                            reg.registers_registerreference,
--                            ri.meterreadingdatetime,
--                            max(ri.meterreadingdatetime) over (partition by su.external_id)                                        as latest_reading_datetime,
--                            min(ri.meterreadingdatetime) over (partition by su.external_id)                                        as earliest_reading_datetime,
--                            row_number() over (partition by su.external_id, reg.register_id order by ri.meterreadingdatetime desc) as latest_read_per_register
--                     from ref_cdb_supply_contracts su
--                            inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'E'
--                            inner join ref_meters mt
--                              on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and
--                                 mt.removeddate is null
--                            inner join ref_registers reg
--                              on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
--                            inner join ref_readings_internal_valid ri
--                              on ri.account_id = su.external_id and ri.register_id = reg.register_id
--                     --where mp.account_id =1831
--                    ) reads
--                      left outer join ref_estimates_elec_internal ee
--                        on ee.account_id = reads.external_id and ee.mpan = reads.meterpointnumber and
--                           ee.register_id = reads.registers_registerreference and
--                           ee.serial_number = reads.meterserialnumber and
--                           ee.effective_from = reads.meterreadingdatetime
--                      left outer join ref_estimates_elec_internal ee1
--                        on ee1.account_id = reads.external_id and ee1.mpan = reads.meterpointnumber and
--                           --   ee1.register_id = reads.registers_registerreference and
--                           --    ee1.serial_number = reads.meterserialnumber and
--                           ee1.effective_from = greatest(reads.associationstartdate, reads.supplystartdate)
--                      left outer join ref_cdb_registrations creg on creg.id = reads.registration_id
--                      left outer join ref_cdb_quotes q on q.id = creg.quote_id) x
--         where x.latest_read_per_register = 1
--          and x.account_id in (9731)
--         group by x.account_id, x.latest_reading_datetime
--         order by x.account_id, x.latest_reading_datetime
--
--
--
-- select mp.account_id, mp.meter_point_id, mt.meter_id, count(*)
--  from ref_meterpoints mp
--         inner join ref_meters mt on mp.account_id = mt.account_id and mp.meter_point_id = mt.meter_point_id
--         inner join ref_registers reg
--           on mt.account_id = reg.account_id and mt.meter_point_id = reg.meter_point_id and mt.meter_id = reg.meter_id
--  where mp.meterpointtype = 'E'
--    and mp.supplyenddate is null
--  and mt.removeddate is null
--  group by mp.account_id, mp.meter_point_id, mt.meter_id
--  having count(*) > 2
--
--
--
--
-- select account_id,
--        reading_datetime,
--        quote_firstaq_var,
--        round(avg(estimate_frequency),0) as estimate_frequency,
--        round(avg(estimate_age),0) as estimate_age,
--        first_ind_aq,
--        pa_cons_gas,
--        igl_ind_aq,
--        ind_aq,
--        quotes_aq,
--        etlchange
-- from (select acc_reg.account_id,
--              acc_reg.first_ind_aq,
--           -- SM creating the variance between quote gas usage and the very 1st registers aq not sure this is right.
--              round(case
--                      when quotes_aq > 0 and acc_reg.first_ind_aq > 0
--                              then 100 -
--                                   (quotes_aq / acc_reg.first_ind_aq * 100)
--                      else NULL end,
--                    3) as                                                                                                                     quote_firstaq_var,
--           -- SM taking the the date diff between between aq's for each register and then averaging
--              avg(
--                datediff(days, acc_reg.current_aq_date, acc_reg.next_aq_date)) over (partition by acc_reg.account_id, acc_reg.register_id) as estimate_frequency,
--           -- SM taking the date diff from the latest aq for all registers
--              datediff(days, acc_reg.latest_aq_date, getdate()) as                                                                            estimate_age,
--              reading_datetime,
--              pa_cons_gas,
--              igl_ind_aq,
--              ind_aq,
--              quotes_aq,
--              etlchange
--       from -- get all registers for an account
--            (select su.external_id                                                                         as account_id,
--                    su.registration_id,
--                    mp.meterpointnumber,
--                    reg.register_id,
--                    mt.meterserialnumber,
--                    reg.registers_registerreference,
--                    mp.supplystartdate,
--                    max(case
--                          when greatest(mp.supplystartdate, mp.associationstartdate) = eg.effective_from
--                                  then estimation_value
--                          else 0 end) over (partition by su.external_id, eg.register_id)                   as first_ind_elec,
--                 -- for calculating frequency
--                    eg.effective_from                                                                         current_elec_date,
--                    lead(eg.effective_from,
--                         1) OVER (PARTITION BY su.external_id, reg.register_id ORDER BY eg.effective_from) as next_elec_date,
--                 -- for calculating age
--                    max(eg.effective_from) over (partition by su.external_id, reg.register_id)                latest_elec_date
--             from ref_cdb_supply_contracts su
--                    inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G'
--                    inner join ref_meters mt
--                      on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and
--                         mt.removeddate is null
--                    inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
--                    left outer join ref_estimates_elec_internal eg on eg.account_id = su.external_id
--                                                                       and mp.meterpointnumber = eg.mprn
--                                                                       and
--                                                                     reg.registers_registerreference = eg.register_id
--                                                                       and mt.meterserialnumber = eg.serial_number
--          --  where su.external_id = 3306
--            ) acc_reg
--              inner join ref_consumption_accuracy_elec cons_elec on cons_elec.account_id = acc_reg.account_id)
-- group by account_id,
--          reading_datetime,
--          quote_firstaq_var,
--          first_ind_aq,
--          pa_cons_gas,
--          igl_ind_aq,
--          ind_aq,
--          quotes_aq,
--          etlchange
-- order by account_id
--
--
select * from ref_consumption_accuracy_elec
order by account_id

select account_id,
       reading_datetime,
       quote_firsteac_var,
       round(avg(estimate_frequency),0) as ind_estimate_frequency,
       round(avg(estimate_age),0) as ind_estimate_age,
       sum(first_ind_eac) as first_ind_eac,
       pa_cons_elec,
       igl_ind_eac,
       ind_eac,
       quotes_eac,
       etlchange
from
     (select acc_reg.account_id,
             acc_reg.first_ind_eac as first_ind_eac,
          -- SM creating the variance between quote gas usage and the very 1st registers aq not sure this is right.
             round(case
                     when quotes_eac > 0 and acc_reg.first_ind_eac > 0
                             then 100 -
                                  (quotes_eac / acc_reg.first_ind_eac * 100)
                     else NULL end,
                   3) as                                                                                                                     quote_firsteac_var,
          -- SM taking the the date diff between between aq's for each register and then averaging
             avg(
               datediff(days, acc_reg.current_elec_date, acc_reg.next_elec_date)) over (partition by acc_reg.account_id, acc_reg.register_id) as estimate_frequency,
          -- SM taking the date diff from the latest aq for all registers
             datediff(days, acc_reg.latest_elec_date, getdate()) as                                                                            estimate_age,
             reading_datetime,
             pa_cons_elec,
             igl_ind_eac,
             ind_eac,
             quotes_eac,
             etlchange
      from -- get all registers for an account
           ref_consumption_accuracy_elec cons_elec inner join
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
                         else 0 end) over (partition by su.external_id, eg.register_id)                   as first_ind_eac,
                -- for calculating frequency
                   eg.effective_from                                                                         current_elec_date,
                   lead(eg.effective_from,
                        1) OVER (PARTITION BY su.external_id, reg.register_id ORDER BY eg.effective_from) as next_elec_date,
                -- for calculating age
                   max(eg.effective_from) over (partition by su.external_id, reg.register_id)                latest_elec_date
            from ref_cdb_supply_contracts su
                   inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'E'
                   inner join ref_meters mt
                     on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id
                   inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
                   left outer join ref_estimates_elec_internal eg on eg.account_id = su.external_id
                                                                      and mp.meterpointnumber = eg.mpan
                                                                      and
                                                                    reg.registers_registerreference = eg.register_id
                                                                      and mt.meterserialnumber = eg.serial_number
           where su.external_id = 1849
           ) acc_reg
              on cons_elec.account_id = acc_reg.account_id)
group by account_id,
         reading_datetime,
         quote_firsteac_var,
         pa_cons_elec,
         igl_ind_eac,
         ind_eac,
         quotes_eac,
         etlchange
order by account_id



-- --
-- --
-- --
-- --
-- -- SELECT account_id, reading_datetime, pa_cons_elec, igl_ind_eac, ind_eac, quotes_eac, etlchange
-- -- FROM ref_consumption_accuracy_elec con_elec
-- -- left outer join  ref_meterpoints mp on con_elec.account_id = mp.account_id;
-- --
-- --
-- --
-- --
-- --
-- -- select * from ref_consumption_accuracy_elec
-- -- where account_id=2117
--
--
--
--
--
--
--
--
-- select * from ref_estimates_elec_internal
-- where account_id = 2117
-- --and islive = true
-- order by effective_from asc
--
-- select * from ref_meterpoints
-- where account_id =2117
--
-- select * from ref_meters
-- where account_id = 2117
--
-- select * from ref_estimates_elec_internal
-- where account_id = 1831
-- --and islive = true
-- order by effective_from asc
--
--
--
--
-- select * from ref_meterpoints
-- where account_id =1831
--
-- select account_id
-- from ref_meterpoints mp
--        inner join (select account_id, count(*)
--                    from ref_registers
--                    group by account_id
--                    having count(*) > 3
--                    order by account_id desc) reg on mp.account_id = reg.account_id
--
--
