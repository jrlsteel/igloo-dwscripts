drop view vw_mmh_responses;
create or replace view vw_mmh_responses as
select distinct surv.title                                                  as survey,
                surv.id                                                     as survey_id,
                surv_cat.title                                              as category,
                surv_cat.id                                                 as category_id,
                surv_quest.title                                            as question,
                surv_quest.id                                               as question_id,
                attr_types.id                                               as attribute_type_id,
                attr_types.entity_type,
                attr_types.attribute_fixed,
                attr.entity_id,
                nvl(attr.attribute_custom_value, attr_vals.attribute_value) as response_value,
                attr.effective_from,
                attr.effective_to
from ref_cdb_surveys surv
         inner join ref_cdb_survey_category surv_cat on surv_cat.survey_id = surv.id
         inner join ref_cdb_survey_questions surv_quest on surv_quest.survey_category_id = surv_cat.id
         inner join ref_cdb_attribute_types attr_types on attr_types.id = surv_quest.attribute_type_id
         inner join ref_cdb_attributes attr on attr.attribute_type_id = attr_types.id
         left join ref_cdb_attribute_values attr_vals on attr_vals.attribute_type_id = attr.attribute_type_id and
                                                         attr_vals.id = attr.attribute_value_id
where surv.slug = 'meandmyhome';

create table temp_mmh_responses as
select *
from vw_mmh_responses;

select count(*)
from vw_mmh_responses
-- 1,147,782

select count(*)
from ref_cdb_attributes
-- 1,658,961

create or replace view vw_user_supply_addresses as
select users.id       as user_id,
       sc.id          as supply_contract_id,
       sc.external_id as ensek_account_id,
       sc.supply_address_id,
       ensek_status.contract_ssd,
       ensek_status.contract_sed,
       ensek_status.contract_status
from ref_cdb_users users
         left join ref_cdb_user_permissions sc_perm on sc_perm.user_id = users.id and
                                                       sc_perm.permissionable_type = 'App\\SupplyContract' and
                                                       sc_perm.permission_level = 0
         left join ref_cdb_supply_contracts sc on sc.id = sc_perm.permissionable_id
         left join vw_ensek_account_supply_status ensek_status
                   on sc.external_id = ensek_status.contract_id;

create table temp_user_supply_addresses as
select *
from vw_user_supply_addresses;

select count(*)
from vw_user_supply_addresses
-- 197,698
;
drop view vw_latest_mmh_responses;
create or replace view vw_latest_mmh_responses as
select users_addresses.user_id,
       users_addresses.supply_contract_id,
       users_addresses.ensek_account_id,
       users_addresses.supply_address_id,
       users_addresses.contract_ssd,
       users_addresses.contract_sed,
       users_addresses.contract_status,

       mmh_resp.question,
       mmh_resp.question_id,
       mmh_resp.entity_type,
       mmh_resp.attribute_fixed,
       mmh_resp.entity_id,
       mmh_resp.response_value,
       greatest(mmh_resp.effective_from, users_addresses.contract_ssd) as effective_from,
       least(mmh_resp.effective_to, users_addresses.contract_sed)      as effective_to

from vw_user_supply_addresses users_addresses
         left join vw_mmh_responses mmh_resp
                   on (users_addresses.user_id = mmh_resp.entity_id and mmh_resp.entity_type = 'App\\User') or
                      (users_addresses.supply_address_id = mmh_resp.entity_id and mmh_resp.entity_type = 'App\\Address')
where nvl(users_addresses.contract_sed, getdate()) between
          mmh_resp.effective_from and nvl(mmh_resp.effective_to, getdate() + 1);

select count(*)
from vw_latest_mmh_responses
-- 1,185,225

create table temp_latest_mmh_responses as
select *
from vw_latest_mmh_responses

select user_id, count(distinct ensek_account_id) as num_ensek
from temp_latest_mmh_responses
group by user_id
having num_ensek > 1