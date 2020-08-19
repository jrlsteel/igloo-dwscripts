select u.id           as user_id,
       u.first_name,
       u.last_name,
       u.email,
       u.phone_number,
       u.mobile_number,
       u.date_of_birth,
       u.password,

       uc.id          as uc_id,
       uc.change_type,
       uc.old_value,
       uc.new_value,

       sc.id          as sc_id,
       sc.external_id as account_id,

       dcf.account_status,
       dcf.signup_channel,
       dcf.signup_channel_secondary,

       r.id           as reg_id,
       r.first_name,
       r.last_name,
       r.email,
       r.phone_number,

       bs.id          as bs_id,
       bs.broker_urn,
       bs.status,

       bih.id         as bih_id,
       bih.data,
       bih.outcome,

       a.id as attr_id,
       a.attribute_type_id,
       a.attribute_value_id,
       a.attribute_custom_value

from users u
         left join user_changes uc on u.id = uc.user_id
         left join user_permissions up_u_sc on up_u_sc.permissionable_type = 'App\\SupplyContract' and
                                               up_u_sc.user_id = u.id
         left join supply_contracts sc on sc.id = up_u_sc.permissionable_id
         left join igloo_datawarehouse.ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
         left join registrations r on sc.registration_id = r.id
         left join broker_signups bs on r.id = bs.registration_id
         left join broker_import_history bih on bs.broker_urn = bih.broker_urn
         left join attributes a
                   on a.attribute_type_id in (1, 9, 21, 24) and a.entity_id = u.id # these types are all user-linked
where u.email like '%REMOVED_[zendesk_id]@example.org'
   or u.email like '[customer_email]%'
order by account_status

delete from attributes where entity_id in (127366) and attribute_type_id in (1, 9, 21, 24)
UPDATE registrations t
SET t.email        = 'REMOVED_1191895@example.org',
    t.phone_number = 'REMOVED_1191895'
WHERE t.id in (136944)
UPDATE users t
SET t.email = concat(t.id, '_REMOVED_1191895@example.org'),
    t.phone_number = 'REMOVED_1191895',
	t.mobile_number = 'REMOVED_1191895',
	t.password = 'REMOVED_1191895',
	t.date_of_birth = null
WHERE t.id in (127366)




