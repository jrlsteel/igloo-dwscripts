create table ref_me_and_my_home_fixed
as
select cs.id as survey_id,
       cs.title as survey_title,
       csr.user_id,
       csr.status,
       up.permissionable_id,
       sc.external_id as account_id,
       sc.supply_address_id,
       csq.title,
       csq.attribute_type_id,
       cav.attribute_value
from ref_cdb_survey_response csr
      inner join ref_cdb_surveys cs on cs.id = csr.survey_id
      inner join ref_cdb_user_permissions up on csr.user_id = up.user_id
      inner join ref_cdb_supply_contracts sc on up.permissionable_id =  sc.id and up.permissionable_type = 'App\\SupplyContract'
      inner join ref_cdb_survey_category csc on cs.id = csc.survey_id
      inner join ref_cdb_survey_questions csq on csc.id = csq.survey_category_id
      inner join ref_cdb_attribute_types cat on csq.attribute_type_id = cat.id
      inner join ref_cdb_attributes ca on cat.id = ca.attribute_type_id and ((ca.entity_id = up.user_id AND ca.entity_type = 'App\\User') OR (ca.entity_id = sc.supply_address_id AND ca.entity_type = 'App\\Address'))
      inner join ref_cdb_attribute_values cav on ca.attribute_value_id = cav.id
where cat.attribute_fixed = True
and csr.status ='completed'
--and  csr.user_id = 2884;