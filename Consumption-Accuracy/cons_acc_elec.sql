select acc_aggs.account_id                      as account_id,
       latest_readings.max_read_datetime        as reading_datetime,
       nvl(acc_aggs.pa_cons_elec, 0)            as pa_cons_elec,
       nvl(acc_aggs.igl_ind_eac, 0)             as igl_ind_eac,
       nvl(acc_aggs.ind_eac, 0)                 as ind_eac,
       nvl(case
               when q.electricity_usage is null then
                       (q.electricity_projected - (3.65 * q.electricity_standing)) / (q.electricity_unit / 100)
               else q.electricity_usage end, 0) as quotes_eac,
       getdate()                                as etlchange
from (select account_id,
             registration_id,
             case
                 when count(nullif(reg_level.pa_cons_elec, 0)) < count(*) then 0
                 else sum(reg_level.pa_cons_elec) end as pa_cons_elec,
             case
                 when count(nullif(reg_level.igl_ind_eac, 0)) < count(*) then 0
                 else sum(reg_level.igl_ind_eac) end  as igl_ind_eac,
             sum(reg_level.ind_eac)                   as ind_eac
      from (select su.external_id           as account_id,
                   su.registration_id,
                   pa_eac.igloo_eac         as pa_cons_elec,
                   calc_eac.igl_ind_eac     as igl_ind_eac,
                   ind_eac.estimation_value as ind_eac
            from vw_supply_contracts_with_occ_accs su
                     inner join ref_meterpoints mp
                                on mp.account_id = su.external_id and mp.meterpointtype = 'E' and
                                   (least(mp.supplyenddate, mp.associationenddate) is null or
                                    least(mp.supplyenddate, mp.associationenddate) >= getdate())
                     inner join ref_meters mt
                                on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and
                                   mt.removeddate is null
                     inner join ref_registers reg
                                on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
                     left join ref_calculated_eac pa_eac
                               on su.external_id = pa_eac.account_id and
                                  reg.register_id = pa_eac.register_id
                     left join ref_calculated_igl_ind_eac calc_eac
                               on su.external_id = calc_eac.account_id and
                                  reg.register_id = calc_eac.register_id
                     left join (select *,
                                       row_number()
                                       over (partition by account_id,
                                           mpan,
                                           register_id,
                                           serial_number
                                           order by effective_from desc) as rn
                                from ref_estimates_elec_internal) ind_eac
                               on ind_eac.rn = 1 and
                                  su.external_id = ind_eac.account_id and
                                  mp.meterpointnumber = ind_eac.mpan and
                                  reg.registers_registerreference = ind_eac.register_id and
                                  mt.meterserialnumber = ind_eac.serial_number
           ) reg_level
      group by account_id, registration_id
     ) acc_aggs
         left join (select account_id, max(meterreadingdatetime) as max_read_datetime
                    from ref_readings_internal_valid
                    where meterpointtype = 'E'
                    group by account_id) latest_readings
                   on latest_readings.account_id = acc_aggs.account_id
         left join ref_cdb_registrations creg on acc_aggs.registration_id = creg.id
         left join ref_cdb_quotes q on q.id = creg.quote_id
where acc_aggs.account_id = 18579
order by account_id