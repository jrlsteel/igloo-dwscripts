create or replace view vw_metering_report_read_schedule as
select scoa.id as sc_id,
       scoa.external_id,
       up.user_id,
       attr.entity_type,
       attr.attribute_type_id,
       attr.attribute_value_id,
       attrv.attribute_value
from vw_supply_contracts_with_occ_accs scoa
         inner join ref_cdb_user_permissions up on scoa.id = up.permissionable_id and
                                                   up.permissionable_type = 'App\\SupplyContract' and
                                                   up.permission_level = 0
         inner join ref_cdb_attributes attr on up.user_id = attr.entity_id and
                                               attr.attribute_type_id = 23 and
                                               getdate() between nvl(attr.effective_from, getdate() - 1) and nvl(attr.effective_to, getdate() + 1)
         inner join ref_cdb_attribute_values attrv on attr.attribute_value_id = attrv.id;

alter table vw_metering_report_read_schedule owner to igloo;