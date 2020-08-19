UPDATE ref_cdb_registrations_audit
SET email        = 'REMOVED_1191895@example.org',
    phone_number = 'REMOVED_1191895'
WHERE id in (136944);

UPDATE ref_cdb_users_audit
SET email         = id || '_REMOVED_1191895@example.org',
    phone_number  = 'REMOVED_1191895',
    mobile_number = 'REMOVED_1191895',
    password      = 'REMOVED_1191895',
    date_of_birth = null
WHERE id in (127366);