select *
from ref_cdb_supply_contracts
where external_id = 54977

select *
from ref_registers
where account_id = 54977

select *
from ref_cdb_addresses
where id = 53261

select *
from ref_cdb_attributes
where attribute_type_id = 2
  and entity_id = 53261

select *
from ref_cdb_attribute_values
where attribute_type_id = 2

select *
from ref_cdb_attribute_types {
     "my_reference":
     "string",
     "location": {
     "latitude": 0,
     "longitude": 0,
     "postcode":
     "string"
    },
     "floor_area": 0,
     "attachment":
     "Detached",
     "renewables": true,
     "heating_type":
     "Other"
    }

select distinct photo_supply
from ref_epc_certificates

select su.external_id                              as account_id,
       up.user_id                                  as user_id,
       addr.uprn,
       lreg.uprn,
       s2epc.lmk_key,
       s2epc.building_reference_number             as brn,
       (select top 1 igloo_aq
        from ref_calculated_aq a
        where su.external_id = a.account_id
        order by a.read_max_created_date_gas desc) as aq,
       s2epc.*
from ref_cdb_supply_contracts su
         inner join ref_cdb_addresses addr on su.supply_address_id = addr.id
         inner join ref_cdb_user_permissions up on su.id = up.permissionable_id and permission_level = 0
    and permissionable_type = 'App\\\SupplyContract'
         inner join ref_cdb_users u on u.id = up.user_id
         left outer join ref_epc_certificates s2epc on trim(addr.postcode) = REPLACE(s2epc.postcode, ' ', '')
         left outer join ref_land_registry lreg on lreg.uprn = addr.uprn
WHERE public.epcaddressmapping(sub_building_name_number,
                               building_name_number,
                               thoroughfare,
                               dependent_locality) = trim(lower(replace(replace(s2epc.address, '- ', ' '), '-', ' ')))

select *
from ref_cdb_user_permissions
where permissionable_type = 'App\\SupplyContract'


select sc.external_id,
       addr.id as my_reference,
       pc.latitude,
       pc.longitude,
       addr.postcode
from ref_cdb_supply_contracts sc
         inner join ref_cdb_addresses addr on sc.supply_address_id = addr.id
         left join ref_postcodes pc on replace(pc.postcode, ' ', '') = replace(addr.postcode, ' ', '')
         left join ref_cdb_registrations r on r.id = sc.registration_id
         left join ref_cdb_quotes q on r.quote_id = q.id
         left join ref_cdb_attributes attr
                   on attr.entity_type ilike 'App%Address' and attr.entity_id = addr.id and attribute_type_id = 5
where sc.external_id = 54977

select entity_id,
       listagg(distinct case attribute_value_id
                            when 29 then 'e'
                            when 30 then 'e'
                            when 32 then 'e'
                            when 33 then 'g'
                            when 26 then 'g'
                            when 31 then 'o'
                            when 28 then 'o'
           end) as fuels
from ref_cdb_attributes
where attribute_type_id = 5
group by entity_id
having len(fuels) > 1

-- 5)
truncate table ref_compare_sql_config;
INSERT INTO ref_compare_sql_config (old_table, new_table, key_cols, destination)
VALUES ('temp_tado_old', 'temp_tado_new', 'user_id, account_id, supply_address_id', 'temp_tado_diffs');

