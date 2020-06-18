SELECT sc.external_id                                                     as account_id,
       start                                                              as meterreadingdatetime,
       replace(replace(endpoint, 'v1/user/meters/', ''), '/readings', '') as meter_id
FROM api_performance api
         left join user_permissions up on up.user_id = api.user_id and up.permission_level = 0 and
                                          up.permissionable_type = 'App\\SupplyContract'
         left join supply_contracts sc on up.permissionable_id = sc.id
where start > '2020-06-17 07:00:00'
  and method = 'POST'
  and endpoint LIKE 'v1/user/meters/%/readings'