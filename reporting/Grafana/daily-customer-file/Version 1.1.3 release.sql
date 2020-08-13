-- update views dependent on DCF to have NO SCHEMA BINDING so the DCF table can be modified with the view in place
drop view if exists vw_tariff_checks;
create or replace view vw_tariff_checks as
    with cte_latest_ensek as
             (select *
              from (select *, row_number() over (partition by account_id, fuel order by start_date desc) as rn
                    from (select eur.account_id,
                                 'E'                   as fuel,
                                 esc.rate              as standing_charge,
                                 eur.rate              as unit_rate,
                                 trunc(eur.start_date) as start_date,
                                 trunc(eur.end_date)   as end_date
                          from public.ref_tariff_history_elec_ur eur
                                   inner join public.ref_tariff_history_elec_sc esc
                                              on eur.account_id = esc.account_id and
                                                 eur.start_date = esc.start_date
                                   inner join public.ref_calculated_daily_customer_file dcf
                                              on eur.account_id = dcf.account_id and
                                                 eur.start_date <= nvl(dcf.elec_ed, getdate() + 1000)
                          union
                          select gur.account_id,
                                 'G'                   as fuel,
                                 gsc.rate              as standing_charge,
                                 gur.rate              as unit_rate,
                                 trunc(gur.start_date) as start_date,
                                 trunc(gur.end_date)   as end_date
                          from public.ref_tariff_history_gas_ur gur
                                   inner join public.ref_tariff_history_gas_sc gsc
                                              on gur.account_id = gsc.account_id and
                                                 gur.start_date = gsc.start_date
                                   inner join public.ref_calculated_daily_customer_file dcf
                                              on gur.account_id = dcf.account_id and
                                                 gur.start_date <= nvl(dcf.gas_ed, getdate() + 1000)
                         ) ensek_tariffs
                   ) numbered_ensek_tariffs
              where rn = 1),
         cte_latest_igloo as
             (select *
              from (select ta.account_id,
                           t.fuel_type                                                                        as fuel,
                           t.standing_charge,
                           t.unit_rate,
                           trunc(ta.start_date)                                                               as start_date,
                           trunc(ta.end_date)                                                                 as end_date,
                           row_number() over (partition by account_id, fuel_type order by ta.start_date desc) as rn
                    from public.ref_calculated_tariff_accounts ta
                             inner join public.ref_tariffs t on ta.tariff_id = t.id) numbered_igloo_tariffs
              where rn = 1),
         cte_account_fuel_pairs as
             (select account_id,
                     'E'             as fuel,
                     trunc(elec_ssd) as ssd,
                     trunc(elec_ed)  as sed,
                     elec_reg_status as reg_status
              from public.ref_calculated_daily_customer_file
              where elec_reg_status is not null
                and elec_reg_status != 'Cancelled'
              union
              select account_id,
                     'G'            as fuel,
                     trunc(gas_ssd) as ssd,
                     trunc(gas_ed)  as sed,
                     gas_reg_status as reg_status
              from public.ref_calculated_daily_customer_file
              where gas_reg_status is not null
                and gas_reg_status != 'Cancelled')
    select afp.*,
           igl_trf.standing_charge is null or igl_trf.unit_rate is null                                 as igl_trf_missing,
           igl_trf.standing_charge                                                                      as igl_trf_standing_charge,
           igl_trf.unit_rate                                                                            as igl_trf_unit_rate,
           igl_trf.start_date                                                                           as igl_trf_start_date,
           igl_trf.end_date                                                                             as igl_trf_end_date,
           ens_trf.standing_charge is null or ens_trf.unit_rate is null                                 as ens_trf_missing,
           ens_trf.standing_charge                                                                      as ens_trf_standing_charge,
           ens_trf.unit_rate                                                                            as ens_trf_unit_rate,
           ens_trf.start_date                                                                           as ens_trf_start_date,
           ens_trf.end_date                                                                             as ens_trf_end_date,
           ens_trf.unit_rate != igl_trf.unit_rate or ens_trf.standing_charge != igl_trf.standing_charge as trf_mismatch,
           case
               when afp.reg_status in ('Live', 'Pending Final') then
                   case
                       when igl_trf_missing or nvl(igl_trf.end_date, getdate() + 1) < getdate()
                           then 'LIVE_IGLOO_MISSING'
                       when ens_trf_missing or nvl(ens_trf.end_date, getdate() + 1) < getdate()
                           then 'LIVE_ENSEK_MISSING'
                       when trf_mismatch then 'LIVE_MISMATCH'
                       else 'Live_Valid'
                       end
               when afp.reg_status = 'Pending Live' then
                   case
                       when igl_trf_missing or igl_trf.start_date > afp.ssd
                           then 'PENDING_IGLOO_MISSING'
                       when datediff(days, getdate(), afp.ssd) <= 14 and
                            (ens_trf_missing or ens_trf.start_date > afp.ssd)
                           then 'PENDING_ENSEK_MISSING'
                       when (not ens_trf_missing) and trf_mismatch then 'PENDING_MISMATCH'
                       else 'Pending_Valid'
                       end
               when afp.reg_status = 'Final' then
                   case
                       when igl_trf_missing or
                            afp.sed not between igl_trf.start_date and nvl(igl_trf.end_date, getdate() + 1)
                           then 'FINAL_IGLOO_MISSING'
                       when ens_trf_missing or
                            afp.sed not between ens_trf.start_date and nvl(ens_trf.end_date, getdate() + 1)
                           then 'FINAL_ENSEK_MISSING'
                       when trf_mismatch then 'FINAL_MISMATCH'
                       else 'Final_Valid'
                       end
               else 'REG_STATUS_INVALID'
               end                                                                                      as error_code,
           getdate()                                                                                    as etlchange
    from cte_account_fuel_pairs afp
             left join cte_latest_ensek ens_trf on afp.account_id = ens_trf.account_id and afp.fuel = ens_trf.fuel
             left join cte_latest_igloo igl_trf on afp.account_id = igl_trf.account_id and afp.fuel = igl_trf.fuel
    order by afp.account_id, afp.fuel
    with no schema binding;
drop view if exists vw_igloo_ensek_tariff_diffs;
create or replace view vw_igloo_ensek_tariff_diffs as
select nvl(igl_trf.account_id, ens_trf.account_id, dcf.account_id) as account_id,
       dcf.account_status,
       dcf.elec_reg_status,
       dcf.gas_reg_status,
       igl_trf.elec_live                                           as igl_elec_live,
       ens_trf.elec_live                                           as ens_elec_live,
       igl_trf.elec_sc                                             as igl_elec_sc,
       ens_trf.elec_sc                                             as ens_elec_sc,
       igl_trf.elec_ur                                             as igl_elec_ur,
       ens_trf.elec_ur                                             as ens_elec_ur,
       igl_trf.gas_live                                            as igl_gas_live,
       ens_trf.gas_live                                            as ens_gas_live,
       igl_trf.gas_sc                                              as igl_gas_sc,
       ens_trf.gas_sc                                              as ens_gas_sc,
       igl_trf.gas_ur                                              as igl_gas_ur,
       ens_trf.gas_ur                                              as ens_gas_ur
from public.ref_calculated_daily_customer_file dcf
         left join (select account_id,
                           elec_tariff_start is not null and
                           nvl(elec_tariff_end, current_timestamp + 1) >= current_timestamp as elec_live,
                           elec_sc,
                           elec_ur,
                           gas_tariff_start is not null and
                           nvl(gas_tariff_end, current_timestamp + 1) >= current_timestamp  as gas_live,
                           gas_sc,
                           gas_ur
                    from public.vw_latest_rates_igloo) igl_trf on dcf.account_id = igl_trf.account_id
         left join (select account_id,
                           elec_tariff_start is not null and
                           nvl(elec_tariff_end, current_timestamp + 1) >= current_timestamp as elec_live,
                           elec_sc,
                           elec_ur,
                           gas_tariff_start is not null and
                           nvl(gas_tariff_end, current_timestamp + 1) >= current_timestamp  as gas_live,
                           gas_sc,
                           gas_ur
                    from public.vw_latest_rates_ensek) ens_trf on dcf.account_id = ens_trf.account_id
where (nvl(dcf.elec_reg_status, '') in ('Live', 'Pending Live', 'Pending Final')
    and (nvl(ens_trf.elec_ur, -1) != nvl(igl_trf.elec_ur, -1)
        or nvl(ens_trf.elec_sc, -1) != nvl(igl_trf.elec_sc, -1)
        or (not ens_trf.elec_live)))
   or (nvl(dcf.gas_reg_status, '') in ('Live', 'Pending Live', 'Pending Final')
    and (nvl(ens_trf.gas_ur, -1) != nvl(igl_trf.gas_ur, -1)
        or nvl(ens_trf.gas_sc, -1) != nvl(igl_trf.gas_sc, -1)
        or (not ens_trf.gas_live)))
order by account_id
    with no schema binding;

-- Add the new signup_credit field before etlchange
alter table ref_calculated_daily_customer_file
    drop column etlchange;
alter table ref_calculated_daily_customer_file
    add column signup_credit double precision;
alter table ref_calculated_daily_customer_file
    add column etlchange timestamp;

alter table vw_tariff_checks
    owner to igloo;
alter table vw_igloo_ensek_tariff_diffs
    owner to igloo;


alter table ref_calculated_metering_report
    drop column etlchange;
alter table ref_calculated_metering_report
    add column lsp_flag varchar(10);
alter table ref_calculated_metering_report
    add column meterpoint_first_seen timestamp;
alter table ref_calculated_metering_report
    add column profile_class varchar(5);
alter table ref_calculated_metering_report
    add column time_pattern_regime varchar(30);
alter table ref_calculated_metering_report
    add column etlchange timestamp;


-- auto-generated definition
create table dwh_manual_batch_accounts
(
    account_id  bigint,
    daily_batch boolean,
    use_from    timestamp,
    use_until   timestamp
);

alter table dwh_manual_batch_accounts
    owner to igloo;
