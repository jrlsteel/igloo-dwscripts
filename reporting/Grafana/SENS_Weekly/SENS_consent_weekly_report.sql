select mpd.id                                                    as ext_participant_id,
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
       listagg(mp.meterpointnumber, ', ')                        as mpan,
       addr.uprn,
       nvl(mpd.opted_out, dcf.acc_ed, getdate() + 1) > getdate() as consent_provided,
       mpd.opted_in                                              as consent_start_date,
       nvl(mpd.opted_out, dcf.acc_ed)                            as consent_end_date,
       'Igloo'                                                   as consent_source,
       mpd.move_in_date,
       'sens-igloo'                                              as trial_id,
       mpd."group"                                               as trial_group
from aws_s3_stage2_extracts.stage2_cdbmeetsprojectdata mpd
         left join ref_cdb_addresses addr on mpd.address_id = addr.id
         left join ref_cdb_users u on mpd.user_id = u.id
         left join ref_cdb_user_permissions up_sc on up_sc.user_id = u.id and up_sc.permission_level = 0 and
                                                     up_sc.permissionable_type ilike 'app%supplycontract'
         left join ref_cdb_supply_contracts sc on up_sc.permissionable_id = sc.id
         left join ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
         left join ref_meterpoints mp on mp.account_id = sc.external_id and mp.meterpointtype = 'E' and
                                         nvl(mpd.opted_out, dcf.acc_ed, trunc(getdate())) between
                                             greatest(supplystartdate, associationstartdate) and
                                             nvl(least(supplyenddate, associationenddate), getdate() + 1)
group by mpd.id, addr.sub_building_name_number, addr.building_name_number, addr.dependent_thoroughfare,
         addr.thoroughfare, addr.double_dependent_locality, addr.dependent_locality, addr.post_town, addr.county,
         addr.postcode, u.first_name, u.last_name, addr.uprn, consent_provided, mpd.opted_in, mpd.opted_out,
         mpd.move_in_date, mpd."group"
order by ext_participant_id