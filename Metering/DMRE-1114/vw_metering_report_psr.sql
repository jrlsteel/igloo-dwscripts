drop view vw_metering_report_psr
create or replace  view vw_metering_report_psr as select scoa.id,
       scoa.external_id,
       up.user_id,
       attr.entity_type,
       attr.attribute_type_id,
       attr.attribute_value_id,
       replace(replace(replace(replace(attr.attribute_custom_value,',',' '), '[', ''), ']', ''), '"', '') as psr
from vw_supply_contracts_with_occ_accs scoa
       inner join ref_cdb_user_permissions up
         on scoa.id = up.permissionable_id and up.permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_attributes attr on up.user_id = attr.entity_id and attr.attribute_type_id = 17


       select scoa.id,
       scoa.external_id,
       up.user_id,
       attr.entity_type,
       attr.attribute_type_id,
       attr.attribute_value_id,
       json_array_length(attr.attribute_custom_value) as psr,
       json_extract_path_text(attr.attribute_custom_value) as psrextract

from vw_supply_contracts_with_occ_accs scoa
       inner join ref_cdb_user_permissions up
         on scoa.id = up.permissionable_id and up.permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_attributes attr on up.user_id = attr.entity_id and attr.attribute_type_id = 17


select
  TRIM(SPLIT_PART(attribute_custom_value, ',', 1)) AS tag

select TRIM(SPLIT_PART(replace(replace(replace(attribute_custom_value, '[', ''), ']', ''), '"', ''), ',', 1))
from ref_cdb_attributes
where ref_cdb_attributes.attribute_type_id = 17


