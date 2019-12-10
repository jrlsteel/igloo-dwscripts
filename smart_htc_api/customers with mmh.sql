select *
from (select rm.account_id, listagg(distinct rm.meterpointtype) types, max(met.removeddate)
      from ref_meterpoints rm
               inner join ref_meters met
                          on rm.meter_point_id = met.meter_point_id and rm.account_id = met.account_id and
                             met.removeddate between '2019-09-01' and '2019-11-20'
      group by rm.account_id
      having len(types) > 1) dual_smart
         inner join
     (select sc.external_id as account_id,
--        sc.id          as sc_id,
--        u.id           as user_id,
--        addr.id        as address_id,
             listagg(distinct attr.attribute_type_id, ',') within group ( order by attr.attribute_type_id )

      from ref_cdb_supply_contracts sc
               left join ref_cdb_addresses addr on sc.supply_address_id = addr.id
               left join ref_cdb_user_permissions up
                         on up.permissionable_type ilike 'app%supplycontract' and up.permissionable_id = sc.id
               left join ref_cdb_users u on u.id = up.user_id
               left join ref_cdb_attributes attr
                         on attr.effective_from <= getdate() and
                            (attr.effective_to is null or attr.effective_to >= getdate()) and
                            ((attr.entity_type ilike 'app%user' and attr.entity_id = u.id) or
                             (attr.entity_type ilike 'app%supplycontract' and attr.entity_id = sc.id) or
                             (attr.entity_type ilike 'app%address' and attr.entity_id = addr.id))
     where attribute_type_id in (1,2,3,4,5,6,7,8,9)
     group by account_id) mmh_completion on mmh_completion.account_id = dual_smart.account_id