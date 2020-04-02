create or replace view vw_sens_consent_weekly_report as
select mpd.id                                                           as ext_participant_id,
       addr.addressline1,
       addr.addressline2,
       addr.addressline3,
       addr.addressline4,
       addr.postaltown,
       addr.county,
       addr.postcode,
       u.first_name                                                     as firstname,
       u.last_name                                                      as lastname,
       listagg(mp.meterpointnumber, ', ')                               as mpan,
       addr.uprn,
       (nvl(mpd.opted_out, dcf.acc_ed, getdate() + 1) > getdate())::int as consent_provided,
       mpd.opted_in                                                     as consent_start_date,
       nvl(mpd.opted_out, dcf.acc_ed)                                   as consent_end_date,
       'Igloo'                                                          as consent_source,
       1                                                                as auth_completed,
       mpd.opted_in                                                     as auth_date,
       mpd.move_in_date                                                 as move_in_date,
       'SENS'                                                           as project_id,
       'sens-igloo'                                                     as trial_id,
       mpd."group"                                                      as trial_group
from aws_s3_stage2_extracts.stage2_cdbmeetsprojectdata mpd
         left join (select id,
                           replace(trim(', ' from nvl(sub_building_name_number, '') ||
                                                  ', ' ||
                                                  nvl(building_name_number, '') ||
                                                  ', ' ||
                                                  nvl(dependent_thoroughfare, '')), ', , ', ', ') as addressline1,
                           thoroughfare                                                           as addressline2,
                           double_dependent_locality                                              as addressline3,
                           dependent_locality                                                     as addressline4,
                           post_town                                                              as postaltown,
                           county,
                           postcode,
                           uprn
                    from public.ref_cdb_addresses) addr on mpd.address_id = addr.id
         left join public.ref_cdb_users u on mpd.user_id = u.id
         left join public.ref_cdb_user_permissions up_sc on up_sc.user_id = u.id and up_sc.permission_level = 0 and
                                                            up_sc.permissionable_type = 'App\\SupplyContract'
         left join public.ref_cdb_supply_contracts sc on up_sc.permissionable_id = sc.id
         left join public.ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
         left join public.ref_meterpoints mp on mp.account_id = sc.external_id and mp.meterpointtype = 'E' and
                                                nvl(mpd.opted_out, dcf.acc_ed, trunc(getdate())) between
                                                    greatest(supplystartdate, associationstartdate) and
                                                    nvl(least(supplyenddate, associationenddate), getdate() + 1)
group by mpd.id, addr.addressline1, addr.addressline2, addr.addressline3, addr.addressline4, addr.postaltown,
         addr.county, addr.postcode, u.first_name, u.last_name, addr.uprn, consent_provided, mpd.opted_in,
         consent_end_date, mpd.move_in_date, mpd."group"
order by ext_participant_id
    with no schema binding;