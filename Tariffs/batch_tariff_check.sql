-- main script
with latest_ensek as
         (select *
          from (select *, row_number() over (partition by account_id, fuel order by start_date desc) as rn
                from (select eur.account_id,
                             'E'                              as fuel,
                             esc.rate                         as standing_charge,
                             eur.rate                         as unit_rate,
                             trunc(eur.start_date)::timestamp as start_date,
                             trunc(eur.end_date)::timestamp   as end_date
                      from ref_tariff_history_elec_ur eur
                               inner join ref_tariff_history_elec_sc esc on eur.account_id = esc.account_id and
                                                                            eur.start_date = esc.start_date
                               inner join ref_calculated_daily_customer_file dcf
                                          on eur.account_id = dcf.account_id and
                                             eur.start_date <= nvl(dcf.elec_ed, getdate() + 1000)
                      union
                      select gur.account_id,
                             'G'                              as fuel,
                             gsc.rate                         as standing_charge,
                             gur.rate                         as unit_rate,
                             trunc(gur.start_date)::timestamp as start_date,
                             trunc(gur.end_date) ::timestamp  as end_date
                      from ref_tariff_history_gas_ur gur
                               inner join ref_tariff_history_gas_sc gsc on gur.account_id = gsc.account_id and
                                                                           gur.start_date = gsc.start_date
                               inner join ref_calculated_daily_customer_file dcf
                                          on gur.account_id = dcf.account_id and
                                             gur.start_date <= nvl(dcf.gas_ed, getdate() + 1000)
                     ) ensek_tariffs
               ) numbered_ensek_tariffs
          where rn = 1),
     latest_igloo as
         (select *
          from (select ta.account_id,
                       t.fuel_type                                                                        as fuel,
                       t.standing_charge,
                       t.unit_rate,
                       trunc(ta.start_date)::timestamp                                                    as start_date,
                       trunc(ta.end_date)::timestamp                                                      as end_date,
                       row_number() over (partition by account_id, fuel_type order by ta.start_date desc) as rn
                from ref_calculated_tariff_accounts ta
                         inner join ref_tariffs t on ta.tariff_id = t.id) numbered_igloo_tariffs
          where rn = 1),
     account_fuel_pairs as
         (select account_id,
                 'E'                        as fuel,
                 trunc(elec_ssd)::timestamp as ssd,
                 trunc(elec_ed)::timestamp  as sed,
                 elec_reg_status            as reg_status
          from ref_calculated_daily_customer_file
          where elec_reg_status is not null
            and elec_reg_status != 'Cancelled'
          union
          select account_id,
                 'G'                       as fuel,
                 trunc(gas_ssd)::timestamp as ssd,
                 trunc(gas_ed)::timestamp  as sed,
                 gas_reg_status            as reg_status
          from ref_calculated_daily_customer_file
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
from account_fuel_pairs afp
         left join latest_ensek ens_trf on afp.account_id = ens_trf.account_id and afp.fuel = ens_trf.fuel
         left join latest_igloo igl_trf on afp.account_id = igl_trf.account_id and afp.fuel = igl_trf.fuel
order by afp.account_id, afp.fuel


-- main table
-- drop table ref_calculated_igl_ens_tariff_comparison;
create table ref_calculated_igl_ens_tariff_comparison
(
    account_id              bigint,
    fuel                    varchar(1),
    ssd                     timestamp,
    sed                     timestamp,
    reg_status              varchar(13),
    igl_trf_missing         boolean,
    igl_trf_standing_charge double precision,
    igl_trf_unit_rate       double precision,
    igl_trf_start_date      timestamp,
    igl_trf_end_date        timestamp,
    ens_trf_missing         boolean,
    ens_trf_standing_charge double precision,
    ens_trf_unit_rate       double precision,
    ens_trf_start_date      timestamp,
    ens_trf_end_date        timestamp,
    trf_mismatch            boolean,
    error_code              varchar(21),
    etlchange               timestamp
)
    sortkey (account_id, fuel);

alter table ref_calculated_igl_ens_tariff_comparison
    owner to igloo;

-- audit table
create table ref_calculated_igl_ens_tariff_comparison_audit
(
    account_id              bigint,
    fuel                    varchar(1),
    ssd                     timestamp,
    sed                     timestamp,
    reg_status              varchar(13),
    igl_trf_missing         boolean,
    igl_trf_standing_charge double precision,
    igl_trf_unit_rate       double precision,
    igl_trf_start_date      timestamp,
    igl_trf_end_date        timestamp,
    ens_trf_missing         boolean,
    ens_trf_standing_charge double precision,
    ens_trf_unit_rate       double precision,
    ens_trf_start_date      timestamp,
    ens_trf_end_date        timestamp,
    trf_mismatch            boolean,
    error_code              varchar(21),
    etlchangetype           varchar(1),
    etlchange               timestamp
)
    sortkey (account_id, fuel);

alter table ref_calculated_igl_ens_tariff_comparison_audit
    owner to igloo;

-- view
create view vw_account_tariff_check_states as
select account_id,
       fuel,
       ssd,
       sed,
       reg_status,
       igl_trf_missing,
       igl_trf_standing_charge,
       igl_trf_unit_rate,
       igl_trf_start_date,
       igl_trf_end_date,
       ens_trf_missing,
       ens_trf_standing_charge,
       ens_trf_unit_rate,
       ens_trf_start_date,
       ens_trf_end_date,
       trf_mismatch,
       error_code,
       etlchange                                    as last_state_change,
       datediff(days, last_state_change, getdate()) as days_in_state
from (select *, row_number() over (partition by account_id, fuel order by etlchange desc) rn
      from ref_calculated_igl_ens_tariff_comparison_audit) ordered
where rn = 1;


select * from ref_calculated_igl_ens_tariff_comparison
select * from vw_tariff_checks

select * from ref_calculated_igl_ens_tariff_comparison_audit

select * from vw_account_tariff_check_states