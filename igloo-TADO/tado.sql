--Batch SQL for igloo-TADO
--Model Outputs and Calculations
select x1.user_id,
       x1.account_id,
       x1.base_temp,
       x1.heating_control,
       x1.heating_basis,
       x1.heating_type,
       x1.heating_source  as fuel_type,
       x1.status,
       x1.estimated_temp,
       x1.base_hours,
       x1.estimate_hours,
       tado_estimate_mean_internal_temp(coalesce(x1.base_temp, 0.0), coalesce(x1.base_hours, 0.0))          as base_mit,
       tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0), coalesce(x1.estimate_hours, 0.0)) as est_mit,
       aq                                                                                                   as est_annual_fuel,
       unit_Rate                                                                                            as unit_rate,
       aq * unit_Rate / 100                                                                                    amount_over_year,
       (
           (
               tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0), coalesce(x1.estimate_hours, 0.0)) -
               tado_estimate_mean_internal_temp(coalesce(x1.base_temp, 0.0), coalesce(x1.base_hours, 0.0))
               ) /
           tado_estimate_mean_internal_temp(coalesce(x1.base_temp, 0.0), coalesce(x1.base_hours, 0.0)) * 100
           )                                                                                                as perc_diff,
       (abs(
          (
              tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0), coalesce(x1.estimate_hours, 0.0)) -
              tado_estimate_mean_internal_temp(coalesce(x1.base_temp, 0.0), coalesce(x1.base_hours, 0.0))
              ) /
          tado_estimate_mean_internal_temp(coalesce(x1.base_temp, 0.0), coalesce(x1.base_hours, 0.0))
            ) * aq * (unit_Rate / 100))                                                                        savings_in_pounds

    --Model 2nd Level Inputs
from (select x.user_id,
             x.account_id,
             cast(max(x.temperature_preference) as double precision)         base_temp,
             max(heating_control_type)                                    as heating_control,
             max(x.heating_basis)                                         as heating_basis,
             max(x.heating_type)                                          as heating_type,
             max(x.heating_source)                                        as heating_source,
             max(x.custom_value)                                          as ages,
             max(x.status)                                                as status,
             max(coalesce(registers_eacaq, 0))                            as aq,
             max(coalesce(tf.rate, 0))                                    as unit_Rate,
             max(q.gas_usage),
             tado_estimate_setpoint_impact(max(x.temperature_preference), max(heating_control_type),
                                           max(x.heating_basis))          as estimated_temp,
             tado_estimate_heating_hours(max(x.heating_type), 'base')     as base_hours,
             tado_estimate_heating_hours(max(x.heating_type), 'estimate') as estimate_hours
    -- Model Inputs
      from (select u.id                      as user_id,
                   su.external_id            as account_id,
                   att.id                    as attribute_type_id,
                   at.entity_type,
                   att.attribute_name        as attribute_name,
                   att.attribute_description,
                   att.attribute_fixed       as fixed,
                   sq.title,
                   av.attribute_value        as fixed_value,
                   case
                     when att.attribute_name = 'temperature_preference' then av.attribute_value
                     else '20' end           as temperature_preference,
                   case
                     when att.attribute_name = 'heating_basis' then av.attribute_value
                     else '' end             as heating_basis,
                   case
                     when att.attribute_name = 'heating_control_type' then av.attribute_value
                     else '' end             as heating_control_type,
                   case
                     when att.attribute_name = 'heating_type' then av.attribute_value
                     else '' end             as heating_source,
                   at.attribute_custom_value as custom_value,
                   sr.status                 as status,
                   case
                     when att.attribute_name = 'resident_ages'
                             then tado_heating_summary(coalesce(at.attribute_custom_value, ''))
                     else '' end             as heating_type
            from ref_cdb_supply_contracts su
                   inner join ref_cdb_user_permissions up on su.id = up.permissionable_id and permission_level = 0
                                                               and permissionable_type = 'App\\SupplyContract'
                   inner join ref_cdb_users u on u.id = up.user_id
                   inner join ref_cdb_attributes at on (
                                                           (at.entity_id = up.user_id AND at.entity_type = 'App\\User')
                                                             OR
                                                           (at.entity_id = su.supply_address_id AND at.entity_type = 'App\\Address')
                                                           )
                                                         and at.effective_to is null
                   inner join ref_cdb_attribute_types att on att.id = at.attribute_type_id and att.effective_to is null
                   left outer join ref_cdb_attribute_values av
                     on av.attribute_type_id = at.attribute_type_id and at.attribute_value_id = av.id
                          and at.attribute_value_id is not null
                   inner join ref_cdb_survey_questions sq on sq.attribute_type_id = att.id
                   inner join ref_cdb_survey_category sc on sc.id = sq.survey_category_id
                   inner join ref_cdb_survey_response sr on sr.user_id = up.user_id and sr.survey_id = sc.survey_id
            where su.external_id = 5918
              and
--     u.id = 24 and
                  att.attribute_name in
                  ('resident_ages', 'heating_control_type', 'temperature_preference', 'heating_basis', 'heating_type')
-- group by u.id, su.external_id
           -- limit 100
           ) x
             left outer join ref_meterpoints mp on mp.account_id = x.account_id and meterpointtype = 'G'
                                                     and (mp.supplyenddate is null or mp.supplyenddate >= getdate())
             inner join ref_meters m
               on m.account_id = mp.account_id and m.meter_point_id = mp.meter_point_id and m.removeddate is null
             inner join ref_registers reg on reg.account_id = mp.account_id and reg.meter_id = m.meter_id
             left outer join ref_tariff_history_gas_ur tf on tf.account_id = x.account_id and tf.end_date is null
             left outer join ref_cdb_quotes q on q.user_id = x.user_id
      group by x.user_id,
               x.account_id) x1;
select count(*)
from ref_cdb_survey_response
where status = 'declined';

select av.attribute_value
from ref_cdb_supply_contracts su
       inner join ref_cdb_user_permissions up on su.id = up.permissionable_id and permission_level = 0
                                                   and permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_users u on u.id = up.user_id
       inner join ref_cdb_survey_response sr on sr.user_id = u.id
       inner join ref_cdb_surveys s on s.id = sr.survey_id
       inner join ref_cdb_survey_category sc on sc.survey_id = s.id
       inner join ref_cdb_survey_questions sq on sq.survey_category_id = sc.id
       inner join ref_cdb_attribute_types at on at.id = sq.attribute_type_id
       inner join ref_cdb_attributes a on at.id = a.attribute_type_id and
                                          (
                                              (a.entity_id = sr.user_id AND a.entity_type = 'App\\User')
                                                OR
                                              (a.entity_id = su.supply_address_id AND a.entity_type = 'App\\Address')
                                              )
       left outer join ref_cdb_attribute_values av on av.id = a.attribute_value_id
where attribute_name = 'heating_type'
group by av.attribute_value
order by av.attribute_value;

select *
from ref_cdb_attribute_types;
select *
from ref_cdb_survey_questions;







