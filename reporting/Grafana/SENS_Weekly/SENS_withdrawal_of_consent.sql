select mpd.id                             as ext_participant_id,
       nvl(mpd.opted_out, dcf.acc_ed)     as consentwithdrawaldate,
       addr.sub_building_name_number,
       addr.building_name_number,
       addr.dependent_thoroughfare,
       addr.thoroughfare,
       addr.double_dependent_locality,
       addr.dependent_locality,
       addr.post_town,
       addr.county,
       addr.postcode,
       u.first_name,
       u.last_name,
       'Online'                           as source,
       'TO BE ADDED'                      as gdprtype,
       listagg(mp.meterpointnumber, ', ') as mpan,
       addr.uprn
from aws_s3_stage2_extracts.stage2_cdbmeetsprojectdata mpd
         left join ref_cdb_addresses addr on mpd.address_id = addr.id
         left join ref_cdb_users u on mpd.user_id = u.id
         left join ref_cdb_user_permissions up_sc on up_sc.user_id = u.id and up_sc.permission_level = 0 and
                                                     up_sc.permissionable_type ilike 'app%supplycontract'
         left join ref_cdb_supply_contracts sc on up_sc.permissionable_id = sc.id
         left join ref_meterpoints mp on mp.account_id = sc.external_id and mp.meterpointtype = 'E' and
                                         nvl(mpd.opted_out, trunc(getdate())) between
                                             greatest(supplystartdate, associationstartdate) and
                                             nvl(least(supplyenddate, associationenddate), getdate() + 1)
         left join ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
where consentwithdrawaldate is not null
group by mpd.id, consentwithdrawaldate, addr.sub_building_name_number, addr.building_name_number, addr.dependent_thoroughfare,
         addr.thoroughfare, addr.double_dependent_locality, addr.dependent_locality, addr.post_town, addr.county,
         addr.postcode, u.first_name, u.last_name, addr.uprn
order by ext_participant_id