drop view vw_metering_report_psr;
create or replace view vw_metering_report_psr as
select scoa.id,
       scoa.external_id,
       up.user_id,
       attr.entity_type,
       attr.attribute_type_id,
       attr.attribute_value_id,
       regexp_replace(attr.attribute_custom_value, '[^0-9\,]') as psr -- remove characters other than digits and commas

from vw_supply_contracts_with_occ_accs scoa
         inner join ref_cdb_user_permissions up
                    on scoa.id = up.permissionable_id and up.permissionable_type = 'App\\SupplyContract'
         inner join ref_cdb_attributes attr on up.user_id = attr.entity_id and attr.attribute_type_id = 17;

alter table vw_metering_report_psr owner to igloo;