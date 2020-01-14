DROP TABLE IF EXISTS ref_me_and_my_home_non_fixed ;
create table ref_me_and_my_home_non_fixed as
select cs.id as survey_id,
       cs.title as survey_title,
       csr.user_id,
       csr.status,
       up.permissionable_id,
       sc.external_id as account_id,
       sc.supply_address_id,
       csq.title,
       csq.attribute_type_id,
       json_horror.*
from ref_cdb_surveys cs
       inner join ref_cdb_survey_response csr on cs.id = csr.survey_id and csr.status ='completed'
       inner join ref_cdb_user_permissions up on csr.user_id = up.user_id and up.permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_supply_contracts sc on up.permissionable_id =  sc.id
       inner join ref_cdb_survey_category csc on cs.id = csc.survey_id
       inner join ref_cdb_survey_questions csq on csc.id = csq.survey_category_id
       inner join ref_cdb_attribute_types cat on csq.attribute_type_id = cat.id
       inner join ref_cdb_attributes ca
                  on cat.id = ca.attribute_type_id and
                     ((ca.entity_id = up.user_id AND ca.entity_type = 'App\\User') OR
                      (ca.entity_id = sc.supply_address_id AND ca.entity_type = 'App\\Address')
                     )
       inner join (select json_objects.id,
       json_objects.entity_type,
       case when json_objects.entity_type = 'App\\Address' then json_extract_path_text(json_objects.elements, 'name') end as machine_name,
       case when json_objects.entity_type = 'App\\Address' then json_extract_path_text(json_objects.elements, 'display_name') end as machine_display_name,
       case when json_objects.entity_type = 'App\\Address' then json_extract_path_text(json_objects.elements, 'usage') end as machine_usage,
       case when json_objects.entity_type = 'App\\Address' then json_extract_path_text(json_objects.elements, 'age') end as machine_age,
       case when json_objects.entity_type = 'App\\User' then json_extract_path_text(json_objects.elements, 'age') end as user_age
      from (
               SELECT id, entity_type, JSON_EXTRACT_ARRAY_ELEMENT_TEXT(replace(attribute_custom_value,'null','""'), seq.i - 1) AS elements
               FROM ref_cdb_attributes,
                     seq_0_to_10 AS seq
                WHERE seq.i < JSON_ARRAY_LENGTH(replace(replace(replace(isnull(attribute_custom_value,'[]'),0,'[]'),'null','""'),'"""',''))
                  and attribute_custom_value is not null and entity_type in ('App\\User', 'App\\Address', 'App\\SupplyContract')
            UNION ALL
               SELECT id, entity_type, attribute_custom_value  AS elements
               FROM ref_cdb_attributes
               WHERE attribute_custom_value is not null and entity_type in ('App\\Bookings\\Models\\BookingAppointment')
           ) as json_objects) json_horror on json_horror.id = ca.id
where cat.attribute_fixed = False
;