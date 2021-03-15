create or replace view vw_sens_weekly_master as
select mpd.id                                                                            as ext_participant_id,
       sc.external_id                                                                    as ensek_id,
       replace(trim(', ' from nvl(addr.sub_building_name_number, '') ||
                              ', ' ||
                              nvl(addr.building_name_number, '') ||
                              ', ' ||
                              nvl(addr.dependent_thoroughfare, '')), ', , ', ', ')       as Address_line1,
       addr.thoroughfare                                                                 as Address_line2,
       addr.double_dependent_locality                                                    as Address_line3,
       addr.dependent_locality                                                           as Address_line4,
       addr.post_town                                                                    as postaltown,
       addr.county,
       addr.postcode,
       u.first_name                                                                      as firstname,
       u.last_name                                                                       as surname,
       meterpoint_summaries.elec_mpans                                                   as mpan,
       addr.uprn,
       (nvl(mpd.opted_out, dcf.acc_ed, getdate() + 1) > getdate())::int                  as consent_provided,
       mpd.opted_in                                                                      as consent_start_date,
       nvl(mpd.opted_out, dcf.acc_ed)                                                    as consent_end_date,
       'Igloo'                                                                           as consent_source,
       1                                                                                 as auth_completed,
       mpd.opted_in                                                                      as auth_date,
       mpd.move_in_date                                                                  as move_in_date,
       'SENS'                                                                            as project_id,
       'sens-igloo'                                                                      as trial_id,
       mpd."group"                                                                       as trial_group,
       -- leave dcc_enrolled_meter as elec only
       case
           when meterpoint_summaries.num_live_elec = meterpoint_summaries.num_live_elec_dcc and
                meterpoint_summaries.num_live_elec > 0 then 'Y'
           else 'N' end                                                                  as dcc_enrolled_meter,
       u.phone_number,
       u.email,
       case when mpd.opted_out is not null then 'OPT_OUT' else dcf.account_loss_type end as withdrawal_type
from aws_s3_stage2_extracts.stage2_cdbmeetsprojectdata mpd
         inner join public.ref_cdb_addresses addr
                    on mpd.address_id = addr.id
         inner join public.ref_cdb_users u
                    on mpd.user_id = u.id
         inner join public.ref_cdb_user_permissions up_sc
                    on up_sc.user_id = u.id and
                       up_sc.permission_level = 0 and
                       up_sc.permissionable_type = 'App\\SupplyContract'
         inner join public.ref_cdb_supply_contracts sc
                    on up_sc.permissionable_id = sc.id and
                       sc.supply_address_id = mpd.address_id
         inner join public.ref_calculated_daily_customer_file dcf
                    on sc.external_id = dcf.account_id and
                       mpd.opted_in between dcf.acc_ssd and nvl(dcf.acc_ed, getdate())
         left join (select account_id,
                           sum((mp.meterpointtype = 'E')::int)                                   as num_live_elec,
                           sum((mp.meterpointtype = 'E' and dev_ids.device_id is not null)::int) as num_live_elec_dcc,
                           sum((mp.meterpointtype = 'G')::int)                                   as num_live_gas,
                           sum((mp.meterpointtype = 'G' and dev_ids.device_id is not null)::int) as num_live_gas_dcc,
                           listagg(distinct case when mp.meterpointtype = 'E' then mp.meterpointnumber end,
                                   ',')                                                          as elec_mpans,
                           listagg(distinct case when mp.meterpointtype = 'G' then mp.meterpointnumber end,
                                   ',')                                                          as gas_mprns
                    from public.ref_meterpoints mp
                             left join public.vw_smart_device_id_mpxn_map dev_ids
                                       on mp.meterpointnumber = dev_ids.mpxn and mp.meterpointtype = dev_ids.fuel
                    where getdate() between
                              greatest(mp.supplystartdate, mp.associationstartdate) and
                              nvl(least(mp.associationenddate, mp.supplyenddate), getdate() + 1)
                    group by account_id) meterpoint_summaries on meterpoint_summaries.account_id = sc.external_id
order by mpd.id
with no schema binding;

alter table vw_sens_weekly_master
    owner to igloo;

grant select on vw_sens_weekly_master to grafana;

grant select on vw_sens_weekly_master to igloo_grafana;

grant select on vw_sens_weekly_master to public;