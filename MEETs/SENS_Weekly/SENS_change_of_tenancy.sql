create or replace view vw_sens_change_of_tenancy as
select mpd.id                             as ext_participant_id,
       dcf.acc_ed                         as dateofhousemove,
       addr.addressline1,
       addr.addressline2,
       addr.addressline3,
       addr.addressline4,
       addr.postaltown,
       addr.county,
       addr.postcode,
       u.first_name,
       u.last_name,
       'Telephone'                        as source,
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
where dcf.acc_ed is not null
group by mpd.id,
         dcf.acc_ed,
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


--from Grafana
create or replace view vw_sens_change_of_tenancy as
select mpd.id                             as ext_participant_id,
       null                               as SERL_participant_ID,
       dcf.acc_ed                         as date_of_house_move,
       case when nvl(addr.sub_building_name_number, '') + ' ' + nvl(addr.building_name_number::text, '') = ''
            then null
             else nvl(addr.sub_building_name_number, '') + ' ' + nvl(addr.building_name_number::text, '')
       end as  Address_line1,
       case when nvl(addr.dependent_thoroughfare, '') + ' ' + nvl(addr.thoroughfare, '') = ''
            then null
             else nvl(addr.dependent_thoroughfare, '') + ' ' + nvl(addr.thoroughfare, '')
       end as  Address_line2,
       addr.double_dependent_locality     as Address_line3,
       addr.dependent_locality            as Address_line4,
       addr.post_town                     as postaltown,
       addr.county,
       addr.postcode,
       u.first_name                       as firstname,
       u.last_name                        as surname,
       'SENS'                             as source,
       listagg(mp.meterpointnumber, ', ') as mpan,
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
where dcf.acc_ed is not null
group by mpd.id, dcf.acc_ed, Address_line1, Address_line2, addr.double_dependent_locality, addr.dependent_locality, addr.post_town, addr.county,
         addr.postcode, u.first_name, u.last_name, addr.uprn
order by mpd.id
with no schema binding