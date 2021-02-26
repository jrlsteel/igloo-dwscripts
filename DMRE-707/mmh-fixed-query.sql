create table ref_me_and_my_home_fixed
as
select surveys.id             as survey_id,
       surveys.title          as survey_title,
       surv_resp.user_id,
       surv_resp.status,
       user_perm.permissionable_id,
       sply_ctrct.external_id as account_id,
       sply_ctrct.supply_address_id,
       surv_quest.title,
       surv_quest.attribute_type_id,
       attr_val.attribute_value
from ref_cdb_survey_response surv_resp
         inner join ref_cdb_surveys surveys on surveys.id = surv_resp.survey_id
         inner join ref_cdb_user_permissions user_perm on surv_resp.user_id = user_perm.user_id
         inner join ref_cdb_supply_contracts sply_ctrct on user_perm.permissionable_id = sply_ctrct.id and
                                                           user_perm.permissionable_type = 'App\\SupplyContract'
         inner join ref_cdb_survey_category surv_cat on surveys.id = surv_cat.survey_id
         inner join ref_cdb_survey_questions surv_quest on surv_cat.id = surv_quest.survey_category_id
         inner join ref_cdb_attribute_types attr_types on surv_quest.attribute_type_id = attr_types.id
         inner join ref_cdb_attributes attr on attr_types.id = attr.attribute_type_id and
                                               ((attr.entity_id = user_perm.user_id AND attr.entity_type = 'App\\User') OR
                                                (attr.entity_id = sply_ctrct.supply_address_id AND
                                                 attr.entity_type = 'App\\Address'))
         inner join ref_cdb_attribute_values attr_val on attr.attribute_value_id = attr_val.id
where attr_types.attribute_fixed = True
  and surv_resp.status = 'completed'
--and  csr.user_id = 2884;

--TODO add clause to check attribute is still current

select distinct status from ref_cdb_survey_response
--TODO add 'started' to status check

--TODO add check for status of supply contract. Users should not be able to see data for an old address I presume