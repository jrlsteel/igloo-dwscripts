select su.external_id           as account_id,
       ac.status ,
       mp_elec.meter_point_id as meter_point_id_elec,
       mp_gas.meter_point_id as meter_point_id_gas,
       mt_elec.meter_id as meter_id_elec,
       mt_gas.meter_id as meter_id_gas,
       reg_elec.register_id as register_id_elec,
       reg_gas.register_id as register_id_gas,
       eac_v1.igloo_eac_v1      as igl_ind_eac,
       aq_v1.igloo_aq_v1        as igl_ind_aq,
       eac_pa.igloo_eac         as pa_cons_elec,
       aq_pa.igloo_aq           as pa_cons_gas,
       reg_elec.registers_eacaq as ind_eac,
       reg_gas.registers_eacaq  as ind_aq ,
       q_eacaq.quotes_eac       as quotes_eac,
       q_eacaq.quotes_aq        as quotes_aq
from ref_cdb_supply_contracts su
       inner join  ref_account_status ac on ac.account_id = su.external_id
       left outer join ref_meterpoints mp_elec on mp_elec.account_id = su.external_id and mp_elec.meterpointtype = 'E'
       left outer join ref_meters mt_elec
         on mp_elec.account_id = mt_elec.account_id and mp_elec.meter_point_id = mt_elec.meter_point_id and
            mt_elec.removeddate is null
       left outer join ref_registers reg_elec
         on mt_elec.account_id = reg_elec.account_id and mt_elec.meter_id = reg_elec.meter_id --Industry aq
       left outer join ref_meterpoints mp_gas on mp_gas.account_id = su.external_id and mp_gas.meterpointtype = 'G'
       left outer join ref_meters mt_gas
         on mp_gas.account_id = mt_gas.account_id and mp_gas.meter_point_id = mt_gas.meter_point_id and
            mt_gas.removeddate is null
       left outer join ref_registers reg_gas on mt_gas.account_id = reg_gas.account_id and
                                                mt_gas.meter_id = reg_gas.meter_id -- igloo elec/gas annualised consumption
       left outer join ref_calculated_eac eac_pa on eac_pa.account_id = su.external_id and eac_pa.register_id = reg_elec.register_id
       left outer join ref_calculated_aq aq_pa on aq_pa.account_id = su.external_id and aq_pa.register_id = reg_gas.register_id--igloo eac / aq
       left outer join ref_calculated_eac_v1 eac_v1 on eac_v1.account_id = su.external_id and eac_v1.register_id = reg_elec.register_id
       left outer join ref_calculated_aq_v1 aq_v1 on aq_v1.account_id = su.external_id and aq_v1.register_id = reg_gas.register_id --quotes elec/gas consumption
       left outer join (select reg.id, q.user_id, gas_usage as quotes_aq, electricity_usage as quotes_eac
                        from ref_cdb_registrations reg
                               inner join ref_cdb_quotes q on q.id = reg.quote_id) q_eacaq
         on q_eacaq.id = su.registration_id

where ac.status = 'Live'
    and su.external_id in (1831, 1838,1846)
order by external_id;


-- select * from ref_calculated_eac_v1 where account_id = 1831;
-- select * from ref_calculated_aq_v1 where account_id = 1831;


--
-- left outer join (select mp.account_id,
--                         sum(case when mp.meterpointtype = 'E' then r.registers_eacaq else 0 end) as ind_eac,
--                         sum(case when mp.meterpointtype = 'G' then r.registers_eacaq else 0 end) as ind_aq
--                   from ref_meterpoints mp
--                   inner join ref_meters m on m.account_id = mp.account_id and m.meter_point_id = mp.meter_point_id and m.removeddate is null
--                   inner join ref_registers r on r.account_id = mp.account_id and m.meter_id = r.meter_id
--                   group by mp.account_id
--                   ) ind_eacaq on ind_eacaq.account_id = su.external_id

drop view vw_consumption_register_accuracy_eac;

create view vw_consumption_register_accuracy_eac
as
select su.external_id           as account_id,
       su.supply_address_id,
       su.registration_id,
       ac.status ,
       mp_elec.meter_point_id as meter_point_id_elec,
       mt_elec.meter_id as meter_id_elec,
       reg_elec.register_id as register_id_elec,
       reg_elec.register_id as register_id,
       eac_v1.igloo_eac_v1      as igl_ind_eac,
       eac_pa.igloo_eac         as pa_cons_elec,
       reg_elec.registers_eacaq as ind_eac--,
     --  q_eacaq.quotes_eac       as quotes_eac
from ref_cdb_supply_contracts su
       inner join  ref_account_status ac on ac.account_id = su.external_id
       inner join ref_meterpoints mp_elec on mp_elec.account_id = su.external_id and mp_elec.meterpointtype = 'E'
       left outer join ref_meters mt_elec
         on mp_elec.account_id = mt_elec.account_id and mp_elec.meter_point_id = mt_elec.meter_point_id and
            mt_elec.removeddate is null
       left outer join ref_registers reg_elec
         on mt_elec.account_id = reg_elec.account_id and mt_elec.meter_point_id = reg_elec.meter_point_id and mt_elec.meter_id = reg_elec.meter_id
       left outer join ref_calculated_eac eac_pa on eac_pa.account_id = su.external_id and eac_pa.register_id = reg_elec.register_id
       left outer join ref_calculated_eac_v1 eac_v1 on eac_v1.account_id = su.external_id and eac_v1.register_id = reg_elec.register_id
--        left outer join (select reg.id, q.user_id, gas_usage as quotes_aq, electricity_usage as quotes_eac
--                         from ref_cdb_registrations reg
--                                inner join ref_cdb_quotes q on q.id = reg.quote_id) q_eacaq
--          on q_eacaq.id = su.registration_id
where ac.status = 'Live'
  --  and su.external_id in (1831, 1838,1846)
order by external_id;

drop view vw_consumption_register_accuracy_aq;

create view vw_consumption_register_accuracy_aq
as
select su.external_id           as account_id,
       su.supply_address_id,
       su.registration_id,
       ac.status ,
       mp_gas.meter_point_id as meter_point_id_gas,
       mt_gas.meter_id as meter_id_gas,
       reg_gas.register_id as register_id_gas,
       aq_v1.igloo_aq_v1        as igl_ind_aq,
       aq_pa.igloo_aq           as pa_cons_gas,
       reg_gas.registers_eacaq  as ind_aq --,
--        q_eacaq.quotes_aq        as quotes_aq
from ref_cdb_supply_contracts su
       inner join  ref_account_status ac on ac.account_id = su.external_id
       inner join ref_meterpoints mp_gas on mp_gas.account_id = su.external_id and mp_gas.meterpointtype = 'G'
       left outer join ref_meters mt_gas
         on mp_gas.account_id = mt_gas.account_id and mp_gas.meter_point_id = mt_gas.meter_point_id and
            mt_gas.removeddate is null
       left outer join ref_registers reg_gas on mt_gas.account_id = reg_gas.account_id and
                                                mt_gas.meter_id = reg_gas.meter_id -- igloo elec/gas annualised consumption
       left outer join ref_calculated_aq aq_pa on aq_pa.account_id = su.external_id and aq_pa.register_id = reg_gas.register_id--igloo eac / aq
       left outer join ref_calculated_aq_v1 aq_v1 on aq_v1.account_id = su.external_id and aq_v1.register_id = reg_gas.register_id --quotes elec/gas consumption
--        left outer join (select reg.id, q.user_id, gas_usage as quotes_aq, electricity_usage as quotes_eac
--                         from ref_cdb_registrations reg
--                                inner join ref_cdb_quotes q on q.id = reg.quote_id) q_eacaq
--          on q_eacaq.id = su.registration_id

where ac.status = 'Live'
  --  and su.external_id in (1831, 1838,1846)
order by external_id;

create view vw_consumption_account_accuracy as
select eac.account_id,
       sum(eac.igl_ind_eac) as igl_ind_eac,
       sum(aq.igl_ind_aq) as igl_ind_aq,
       sum(eac.pa_cons_elec) as pa_cons_elec,
       sum(aq.pa_cons_gas) as pa_cons_gas,
       sum(eac.ind_eac) as ind_eac,
       sum(aq.ind_aq) as ind_aq,
       q_eacaq.quotes_eac as quotes_eac,
       q_eacaq.quotes_aq  as quotes_aq
from vw_consumption_register_accuracy_eac eac
            left outer join vw_consumption_register_accuracy_aq aq on eac.account_id = aq.account_id
            left outer join (select reg.id, q.user_id, gas_usage as quotes_aq, electricity_usage as quotes_eac
                             from ref_cdb_registrations reg
                                         inner join ref_cdb_quotes q on q.id = reg.quote_id) q_eacaq
         on q_eacaq.id = eac.registration_id
group by eac.account_id, q_eacaq.quotes_eac, q_eacaq.quotes_aq
order by eac.account_id;

drop view vw_consumption_account_accuracy_tolerances;

create view vw_consumption_account_accuracy_tolerances as
  SELECT account_id,
         igl_ind_eac,
         (case
            when (igl_ind_eac is null or pa_cons_elec is null) or (igl_ind_eac = 0 or pa_cons_elec = 0) then -98
            else igl_ind_eac / pa_cons_elec end) - 1 as igl_ind_eac_tolerance_cons,
         (case
            when (igl_ind_eac is null or ind_eac is null) or (igl_ind_eac = 0 or ind_eac = 0) then -98
            else igl_ind_eac / ind_eac end) - 1      as igl_ind_eac_tolerance_ind,
         igl_ind_aq,
         (case
            when (igl_ind_aq is null or pa_cons_gas is null) or (igl_ind_aq = 0 or pa_cons_gas = 0) then -98
            else igl_ind_aq / pa_cons_gas end) - 1   as igl_ind_aq_tolerance_cons,
         (case
            when (igl_ind_aq is null or ind_aq is null) or (igl_ind_aq = 0 or ind_aq = 0) then -98
            else igl_ind_aq / ind_aq end) - 1        as igl_ind_aq_tolerance_ind,
         pa_cons_elec,
         (case
            when (pa_cons_elec is null or ind_eac is null) or (pa_cons_elec = 0 or ind_eac = 0) then -98
            else pa_cons_elec / ind_eac end) - 1     as pa_cons_elec_tolerance_ind,
         (case
            when (pa_cons_elec is null or quotes_eac is null) or (pa_cons_elec = 0 or quotes_eac = 0) then -98
            else pa_cons_elec / quotes_eac end) - 1  as pa_cons_elec_tolerance_quotes,
         pa_cons_gas,
         (case
            when (pa_cons_gas is null or ind_aq is null) or (pa_cons_gas = 0 or ind_aq = 0) then -98
            else pa_cons_gas / ind_aq end) - 1       as pa_cons_gas_tolerance_ind,
         (case
            when (pa_cons_gas is null or quotes_aq is null) or (pa_cons_gas = 0 or quotes_aq = 0) then -98
            else pa_cons_gas / quotes_aq end) - 1    as pa_cons_gas_tolerance_quotes,
         ind_eac,
         (case
            when (ind_eac is null or quotes_eac is null) or (ind_eac = 0 or quotes_eac = 0) then -98
            else ind_eac / quotes_eac end) - 1       as ind_eac_tolerance_quotes,
         ind_aq,
         (case
            when (ind_aq is null or quotes_aq is null) or (ind_aq = 0 or quotes_aq = 0) then -98
            else ind_aq / quotes_aq end) - 1         as ind_aq_tolerance_quotes,
         quotes_eac,
         quotes_aq
  FROM vw_consumption_account_accuracy


SELECT account_id,
       igl_ind_eac,
       (case
          when (igl_ind_eac is null or pa_cons_elec is null) or (igl_ind_eac = 0 or pa_cons_elec = 0) then -98
          else igl_ind_eac / pa_cons_elec end) - 1 as igl_ind_eac_tolerance_cons,
       case
         when ((case
                  when (igl_ind_eac is null or pa_cons_elec is null) or (igl_ind_eac = 0 or pa_cons_elec = 0) then -98
                  else igl_ind_eac / pa_cons_elec end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as igl_ind_eac_tolerance_cons_flag,
       (case
          when (igl_ind_eac is null or ind_eac is null) or (igl_ind_eac = 0 or ind_eac = 0) then -98
          else igl_ind_eac / ind_eac end) - 1      as igl_ind_eac_tolerance_ind,
       case
         when ((case
                  when (igl_ind_eac is null or ind_eac is null) or (igl_ind_eac = 0 or ind_eac = 0) then -98
                  else igl_ind_eac / ind_eac end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as igl_ind_eac_tolerance_ind_flag,
       igl_ind_aq,
       (case
          when (igl_ind_aq is null or pa_cons_gas is null) or (igl_ind_aq = 0 or pa_cons_gas = 0) then -98
          else igl_ind_aq / pa_cons_gas end) - 1   as igl_ind_aq_tolerance_cons,
       case
         when ((case
                  when (igl_ind_aq is null or pa_cons_gas is null) or (igl_ind_aq = 0 or pa_cons_gas = 0) then -98
                  else igl_ind_aq / pa_cons_gas end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as igl_ind_aq_tolerance_cons_flag,
       (case
          when (igl_ind_aq is null or ind_aq is null) or (igl_ind_aq = 0 or ind_aq = 0) then -98
          else igl_ind_aq / ind_aq end) - 1        as igl_ind_aq_tolerance_ind,
       case
         when ((case
                  when (igl_ind_aq is null or ind_aq is null) or (igl_ind_aq = 0 or ind_aq = 0) then -98
                  else igl_ind_aq / ind_aq end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as igl_ind_aq_tolerance_ind_flag,
       pa_cons_elec,
       (case
          when (pa_cons_elec is null or ind_eac is null) or (pa_cons_elec = 0 or ind_eac = 0) then -98
          else pa_cons_elec / ind_eac end) - 1     as pa_cons_elec_tolerance_ind,
       case
         when ((case
                  when (pa_cons_elec is null or ind_eac is null) or (pa_cons_elec = 0 or ind_eac = 0) then -98
                  else pa_cons_elec / ind_eac end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as pa_cons_elec_tolerance_ind_flag,
       (case
          when (pa_cons_elec is null or quotes_eac is null) or (pa_cons_elec = 0 or quotes_eac = 0) then -98
          else pa_cons_elec / quotes_eac end) - 1  as pa_cons_elec_tolerance_quotes,
       case
         when ((case
                  when (pa_cons_elec is null or quotes_eac is null) or (pa_cons_elec = 0 or quotes_eac = 0) then -98
                  else pa_cons_elec / quotes_eac end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as pa_cons_elec_tolerance_quotes_flag,
       pa_cons_gas,
       (case
          when (pa_cons_gas is null or ind_aq is null) or (pa_cons_gas = 0 or ind_aq = 0) then -98
          else pa_cons_gas / ind_aq end) - 1       as pa_cons_gas_tolerance_ind,
       case
         when ((case
                  when (pa_cons_gas is null or ind_aq is null) or (pa_cons_gas = 0 or ind_aq = 0) then -98
                  else pa_cons_gas / ind_aq end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as pa_cons_gas_tolerance_ind_flag,
       (case
          when (pa_cons_gas is null or quotes_aq is null) or (pa_cons_gas = 0 or quotes_aq = 0) then -98
          else pa_cons_gas / quotes_aq end) - 1    as pa_cons_gas_tolerance_quotes,
       case
         when ((case
                  when (pa_cons_gas is null or quotes_aq is null) or (pa_cons_gas = 0 or quotes_aq = 0) then -98
                  else pa_cons_gas / quotes_aq end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as pa_cons_gas_tolerance_quotes_flag,
       ind_eac,
       (case
          when (ind_eac is null or quotes_eac is null) or (ind_eac = 0 or quotes_eac = 0) then -98
          else ind_eac / quotes_eac end) - 1       as ind_eac_tolerance_quotes,
       case
         when ((case
                  when (ind_eac is null or quotes_eac is null) or (ind_eac = 0 or quotes_eac = 0) then -98
                  else ind_eac / quotes_eac end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as ind_eac_tolerance_quotes_flag,
       ind_aq,
       (case
          when (ind_aq is null or quotes_aq is null) or (ind_aq = 0 or quotes_aq = 0) then -98
          else ind_aq / quotes_aq end) - 1         as ind_aq_tolerance_quotes,
       case
         when ((case
                  when (ind_aq is null or quotes_aq is null) or (ind_aq = 0 or quotes_aq = 0) then -98
                  else ind_aq / quotes_aq end) - 1) between -0.15 and 0.15 then 1
         else 0 end                                as ind_aq_tolerance_quotes_flag,
       quotes_eac,
       quotes_aq
FROM vw_consumption_account_accuracy