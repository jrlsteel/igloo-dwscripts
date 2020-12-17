create or replace view vw_sens_withdrawal_of_consent as
select mpd.id                             as ext_participant_id,
       nvl(mpd.opted_out, dcf.acc_ed)     as consentwithdrawaldate,
       addr.addressline1,
       addr.addressline2,
       addr.addressline3,
       addr.addressline4,
       addr.postaltown,
       addr.county,
       addr.postcode,
       u.first_name,
       u.last_name,
       'Online'                           as source,
       'ConsentWithdrawal'                as gdprtype,
       listagg(mp.meterpointnumber, ', ') as mpan,
       addr.uprn
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
                                                            up_sc.permissionable_type ilike 'app%supplycontract'
         left join public.ref_cdb_supply_contracts sc on up_sc.permissionable_id = sc.id
         left join public.ref_meterpoints mp on mp.account_id = sc.external_id and mp.meterpointtype = 'E' and
                                                nvl(mpd.opted_out, trunc(getdate())) between
                                                    greatest(supplystartdate, associationstartdate) and
                                                    nvl(least(supplyenddate, associationenddate), getdate() + 1)
         left join public.ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
where consentwithdrawaldate is not null
group by mpd.id,
         consentwithdrawaldate,
         addr.addressline1,
         addr.addressline2,
         addr.addressline3,
         addr.addressline4,
         addr.postaltown,
         addr.county,
         addr.postcode,
         u.first_name,
         u.last_name,
         addr.uprn
order by mpd.id
with no schema binding


-- from Grafana
create or replace view vw_sens_withdrawal_of_consent as
select mpd.id                              as ext_participant_id,
       null                                as SERL_participant_ID,
       coalesce(mpd.opted_out, dcf.acc_ed) as Consent_Withdrawal_Date,
       --addr.sub_building_name_number,
       addr.building_name_number           as Address_line1,
       --addr.dependent_thoroughfare,
       addr.thoroughfare                   as Address_line2,
       addr.double_dependent_locality      as Address_line3,
       addr.dependent_locality             as Address_line4,
       addr.post_town                      as postaltown,
       addr.county,
       addr.postcode,
       u.first_name                        as firstname,
       u.last_name                         as surname,
       'SENS'                              as source,
       'FullErasure'                       as gdprtype,
       listagg(mp.meterpointnumber, ', ')  as mpan,
       addr.uprn
from aws_s3_stage2_extracts.stage2_cdbmeetsprojectdata mpd
         left join public.ref_cdb_addresses addr on mpd.address_id = addr.id
         left join public.ref_cdb_users u on mpd.user_id = u.id
         left join public.ref_cdb_user_permissions up_sc on up_sc.user_id = u.id and up_sc.permission_level = 0 and
                                                     up_sc.permissionable_type ilike 'app%supplycontract'
         left join public.ref_cdb_supply_contracts sc on up_sc.permissionable_id = sc.id
         left join public.ref_meterpoints mp on mp.account_id = sc.external_id and mp.meterpointtype = 'E' and
                                         nvl(mpd.opted_out, trunc(getdate())) between
                                             greatest(supplystartdate, associationstartdate) and
                                             nvl(least(supplyenddate, associationenddate), getdate() + 1)
         left join public.ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
where Consent_Withdrawal_Date is not null
  AND dcf.acc_ed is null
group by mpd.id, Consent_Withdrawal_Date,
         --addr.sub_building_name_number,
         addr.building_name_number,
         --addr.dependent_thoroughfare,
         addr.thoroughfare, addr.double_dependent_locality, addr.dependent_locality, addr.post_town, addr.county,
         addr.postcode, u.first_name, u.last_name, addr.uprn
order by mpd.id
with no schema binding