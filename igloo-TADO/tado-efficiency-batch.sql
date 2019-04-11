--Batch SQL for igloo-TADO
--Model Outputs and Calculations
drop table ref_calculated_tado_efficiency_batch;

create table ref_calculated_tado_efficiency_batch as
 select x1.user_id,
                   x1.account_id,
                   x1.base_temp,
                   x1.heating_basis,
                   x1.heating_control,
                   x1.heating_source,
                   x1.ages,
                   x1.status,
                   x1.family_category,
                   x1.base_temp_used,
                   x1.estimated_temp,
                   x1.base_hours,
                   x1.estimated_hours,
                   tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0), coalesce(x1.base_hours, 0.0)) as base_mit,
                   tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0),
                                                    coalesce(x1.estimated_hours, 0.0))                              as estimated_mit,
                   est_annual_source,
                   est_annual_fuel_used,
                   (select avg_perc_diff from ref_calculated_tado_efficiency_average)                               as igloo_avg_perc_diff,
                   (select avg_perc_diff from ref_calculated_tado_efficiency_average)                               as igloo_avg_savings,
                   (unit_Rate + (unit_Rate * .05)) / 100                                                            as unit_rate_with_vat,
                   est_annual_fuel_used * (unit_Rate + (unit_Rate * .05)) / 100                                        amount_over_year,
                   (
                       (
                           tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0), coalesce(x1.estimated_hours, 0.0)) -
                           tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0), coalesce(x1.base_hours, 0.0))
                           ) /
                       tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0), coalesce(x1.base_hours, 0.0)) * 100
                       )                                                                                            as perc_diff,
                   ((
                        (
                            tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0), coalesce(x1.estimated_hours, 0.0)) -
                            tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0), coalesce(x1.base_hours, 0.0))
                            ) /
                        tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0), coalesce(x1.base_hours, 0.0))
                        ) * est_annual_fuel_used *
                    ((unit_Rate + (unit_Rate * .05)) / 100))                                                           savings_in_pounds,
                   tado_savings_segmentation(x1.base_temp,
                                             x1.heating_basis,
                                             x1.heating_control,
                                             x1.family_category,
                                             x1.heating_source,
                                             ((
                                                  (
                                                      tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0),
                                                                                       coalesce(x1.estimated_hours, 0.0)) -
                                                      tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0),
                                                                                       coalesce(x1.base_hours, 0.0))
                                                      ) /
                                                  tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0),
                                                                                   coalesce(x1.base_hours, 0.0))
                                                  ) * est_annual_fuel_used *
                                              ((unit_Rate + (unit_Rate * .05)) / 100)))                             as segment,
                   getdate()                                                                                        as etlchange
                --Model 2nd Level Inputs
            from (select x.user_id,
                         x.account_id,
                         x.base_temp,
                         case
                           when x.base_temp <= 0 then x.default_base_temp
                           else x.base_temp end                                                   as base_temp_used,
                         case
                           when heating_control_type = '' then 'unknown'
                           else x.heating_control_type end                                        as heating_control,
                         case when x.heating_basis = '' then 'unknown' else x.heating_basis end   as heating_basis,
                         case
                           when x.family_category = '' then 'unknown'
                           else x.family_category end                                             as family_category,
                         case when x.heating_source = '' then 'unknown' else x.heating_source end as heating_source,
                         case when x.ages = '' then 'unknown' else x.ages end                     as ages,
                         x.status                                                                 as status,
                         coalesce(tf.rate, 0)                                                     as unit_Rate,
                         case when x.aq = 0 then 2  else 1 end                                    as est_annual_source,
                         case when x.aq = 0 then x.gas_usage else x.aq end                        as est_annual_fuel_used,
                         tado_estimate_setpoint_impact(case
                                                         when x.base_temp <= 0 then x.default_base_temp
                                                         else x.base_temp end,
                                                       x.heating_control_type,
                                                       x.heating_basis)                           as estimated_temp,
                         tado_estimate_heating_hours(coalesce(x.family_category, ''), 'base')     as base_hours,
                         tado_estimate_heating_hours(coalesce(x.family_category, ''), 'estimate') as estimated_hours
                -- Model 1st Level Inputs
                  from (select u.id                                            as user_id,
                               su.external_id                                  as account_id,
                               cast(max(case
                                          when att.attribute_name = 'temperature_preference' then av.attribute_value
                                          else '-99' end) as double precision) as base_temp,
                               20.00                                           as default_base_temp,
                               max(case
                                     when att.attribute_name = 'heating_basis' then av.attribute_value
                                     else '' end)                              as heating_basis,
                               max(case
                                     when att.attribute_name = 'heating_control_type' then av.attribute_value
                                     else '' end)                              as heating_control_type,
                               max(case
                                     when att.attribute_name = 'heating_type' then av.attribute_value
                                     else '' end)                              as heating_source,
                               coalesce(max(at.attribute_custom_value), '')    as ages,
                               max(sr.status)                                  as status,
                               max(case
                                     when att.attribute_name = 'resident_ages'
                                             then tado_heating_summary(coalesce(at.attribute_custom_value, ''))
                                     else '' end)                              as family_category,
                               coalesce((select sum(reg.registers_eacaq)
                                         from ref_meterpoints mp
                                                inner join ref_meters m
                                                  on m.account_id = mp.account_id and m.meter_point_id = mp.meter_point_id and
                                                     m.removeddate is null
                                                inner join ref_registers reg
                                                  on reg.account_id = mp.account_id and reg.meter_id = m.meter_id
                                         where mp.account_id = su.external_id
                                           and mp.meterpointtype = 'G'
                                           and (mp.supplyenddate is null or mp.supplyenddate >= getdate())
                                         group by mp.account_id), 0)           as aq,
                               coalesce((select q.gas_usage
                                         from ref_cdb_registrations r
                                                inner join ref_cdb_quotes q on r.quote_id = q.id
                                         where r.id = su.external_id
                                         order by q.updated_at desc
                                         limit 1), 0)                          as gas_usage
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
                        where
                            --     u.id = 24 and
                              att.attribute_name in
                              ('resident_ages', 'heating_control_type', 'temperature_preference', 'heating_basis', 'heating_type')
                        group by u.id, su.external_id) x
                         left outer join ref_tariff_history_gas_ur tf on tf.account_id = x.account_id and tf.end_date is null) x1;

