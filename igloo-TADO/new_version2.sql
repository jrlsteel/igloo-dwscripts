drop table temp_tado_update;
create table temp_tado_update as
select x2.user_id,
       x2.account_id,
       x2.supply_address_id,
       x2.postcode,
       x2.fuel_type,
       x2.base_temp,
       x2.heating_basis,
       x2.heating_control_type,
       x2.heating_source,
       x2.house_bedrooms,
       x2.house_type,
       x2.house_age,
       x2.ages,
       x2.family_category,
       x2.mmh_tado_status,
       x2.base_temp_used,
       x2.estimated_temp,
       x2.base_hours,
       x2.estimated_hours,
       x2.base_mit,
       x2.estimated_mit,
       x2.mmhkw_heating_source,
       x2.mmhkw_floor_area_band,
       x2.mmhkw_prop_age_id,
       x2.mmhkw_prop_type_id,
       x2.region_id,
       x2.annual_consumption,
       x2.annual_consumption_source,
       x2.unit_rate_with_vat,
       x2.unit_rate_source,
       x2.savings_perc,
       x2.avg_savings_perc,
       case
           when x2.mmh_tado_status in ('nodata', 'incomplete') then 'avg_perc'
           else 'tado_perc'
           end                                       as savings_perc_source,
       x2.annual_consumption * x2.unit_rate_with_vat as amount_over_year,
       case
           when x2.mmh_tado_status in ('nodata', 'incomplete') then
                   (x2.annual_consumption * x2.unit_rate_with_vat * x2.avg_savings_perc) / 100
           else (x2.annual_consumption * x2.unit_rate_with_vat * x2.savings_perc) / 100
           end                                       as savings_in_pounds,
       tado_savings_segmentation(
               x2.mmh_tado_status,
               x2.fuel_type,
               x2.heating_control_type,
               x2.heating_source,
               (case
                    when x2.mmh_tado_status in ('nodata', 'incomplete') then
                            (x2.annual_consumption * x2.unit_rate_with_vat * x2.avg_savings_perc) / 100
                    else (x2.annual_consumption * x2.unit_rate_with_vat * x2.savings_perc) / 100
                   end
                   ))                                as segment,
       getdate()                                     as etlchange


--into #temp_tado_eff ---- TEMP TABLE ---

--Model 3rd Level Inputs
from (select x1.*,
             tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0),
                                              coalesce(x1.base_hours, 0.0))               as base_mit,
             tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0),
                                              coalesce(x1.estimated_hours, 0.0))          as estimated_mit,
             (
                         (
                                 tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0),
                                                                  coalesce(x1.estimated_hours, 0.0)) -
                                 tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0),
                                                                  coalesce(x1.base_hours, 0.0))
                             ) /
                         tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0),
                                                          coalesce(x1.base_hours, 0.0)) * 100
                 )                                                                        as savings_perc,
             (select avg_perc_diff from ref_calculated_tado_efficiency_average)           as avg_savings_perc,
             case
                 when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler')
                     or x1.heating_source = 'oilboiler' then (select gas_usage_gas_heating
                                                              from ref_cdb_mmh_need_profiling_lookup
                                                              where region_id = x1.region_id
                                                                and prop_type = x1.mmhkw_prop_type_id
                                                                and prop_age = x1.mmhkw_prop_age_id
                                                                and floor_area_band = x1.mmhkw_floor_area_band)
                 else case
                          when x1.gas_consumption_accuracy_value != 0 then x1.gas_consumption_accuracy_value
                          else (select gas_usage_gas_heating
                                from ref_cdb_mmh_need_profiling_lookup
                                where region_id = x1.region_id
                                  and prop_type = x1.mmhkw_prop_type_id
                                  and prop_age = x1.mmhkw_prop_age_id
                                  and floor_area_band = x1.mmhkw_floor_area_band) end end as annual_consumption,
             case
                 when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler')
                     or x1.heating_source = 'oilboiler' then 'mmh_kwh' --mmh_like_yours
                 else case
                          when x1.gas_consumption_accuracy_value != 0 then x1.gas_consumption_accuracy_type -- aq
                          else 'mmh_kwh' --mmh_like_yours
                     end end                                                              as annual_consumption_source,
             case
                 when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler')
                     then (unit_rate_gas_offgrid + (unit_rate_gas_offgrid * .05)) / 100
                 else case
                          when x1.heating_source = 'oilboiler'
                              then (unit_rate_oil + (unit_rate_oil * .05)) / 100
                          else (unit_rate_gas + (unit_rate_gas * .05)) / 100 end end      as unit_rate_with_vat,
             case
                 when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler') then 'gas_offgrid'
                 else case
                          when x1.heating_source = 'oilboiler' then 'oil'
                          else 'gas' end end                                              as unit_rate_source


             --Model 2nd Level Inputs
      from (select x.user_id,
                   x.account_id,
                   x.supply_address_id,
                   x.postcode,
                   x.base_temp,
                   case
                       when x.base_temp <= 0 then x.default_base_temp
                       else x.base_temp end                                                 as base_temp_used,
                   case
                       when heating_control_type = '' then 'unknown'
                       else x.heating_control_type end                                      as heating_control_type,
                   case when x.heating_basis = '' then 'unknown' else x.heating_basis end   as heating_basis,
                   case
                       when x.family_category = '' or x.family_category = 'unknown' then 'unknown'
                       else x.family_category end                                           as family_category,
                   case when x.heating_source = '' then 'unknown' else x.heating_source end as heating_source,
                   case when x.house_bedrooms = '' then 'unknown' else x.house_bedrooms end as house_bedrooms,
                   case when x.house_type = '' then 'unknown' else x.house_type end         as house_type,
                   case when x.house_age = '' then 'unknown' else x.house_age end           as house_age,
                   case when x.ages = '' then 'unknown' else x.ages end                     as ages,
                   (case
                        when x.sr_user_id is null
                            then 'nodata'
                        else case
                                 when x.base_temp <= 0 or heating_control_type = '' or x.heating_basis = '' or
                                      x.family_category in ('', 'unknown') or
                                      x.heating_source = '' or x.ages = ''
                                     then 'incomplete'
                                 else 'complete' end end)                                   as mmh_tado_status,
                   x.fuel_type,
                   tado_estimate_setpoint_impact(
                           case
                               when x.base_temp <= 0 then x.default_base_temp
                               else x.base_temp end,
                           x.heating_control_type,
                           x.heating_basis)                                                 as estimated_temp,
                   tado_estimate_heating_hours(coalesce(x.family_category, ''), 'base')     as base_hours,
                   tado_estimate_heating_hours(coalesce(x.family_category, ''), 'estimate') as estimated_hours,
                   tado_get_heating_source(coalesce(x.heating_source, ''))                  as mmhkw_heating_source,
                   tado_get_prop_floor_area_band(coalesce(x.house_bedrooms, ''),
                                                 coalesce(x.house_type, ''))                as mmhkw_floor_area_band,
                   tado_get_prop_age_id(coalesce(x.house_age, ''))                          as mmhkw_prop_age_id,
                   tado_get_prop_type_id(coalesce(x.house_type, ''))                        as mmhkw_prop_type_id,
                   mmhpc.region_id                                                          as region_id,
                   coalesce(tf.rate, 0)                                                     as unit_rate_gas,
                   (select fuel_rate from ref_calculated_tado_fuel where fuel_id = 1)       as unit_rate_oil,
                   (select fuel_rate from ref_calculated_tado_fuel where fuel_id = 2)       as unit_rate_gas_offgrid,
                   get_best_consumption(coalesce(rcag.igl_ind_aq, 0), coalesce(rcag.ind_aq, 0),
                                        coalesce(rcag.pa_cons_gas, 0), coalesce(rcag.quotes_aq, 0),
                                        'gas')                                              as gas_consumption_accuracy_type,
                   case
                       when gas_consumption_accuracy_type = 'igl_ind_aq' then rcag.igl_ind_aq
                       when gas_consumption_accuracy_type = 'ind_aq' then rcag.ind_aq
                       when gas_consumption_accuracy_type = 'pa_cons_gas' then rcag.pa_cons_gas
                       when gas_consumption_accuracy_type = 'quotes_aq' then rcag.quotes_aq
                       else 0 end                                                           as gas_consumption_accuracy_value


                   -- Model 1st Level Inputs
            from (with current_attributes as (
                select attr.entity_id,
                       attr.entity_type,
                       case
                           when attr.entity_type ilike 'app%user' then entity_id
                           else null end                                             as user_id,
                       case
                           when attr.entity_type ilike 'app%supplycontract' then entity_id
                           else null end                                             as supply_contract_id,
                       case
                           when attr.entity_type ilike 'app%address' then entity_id
                           else null end                                             as address_id,
                       attr.attribute_type_id,
                       attr_t.attribute_name,
                       attr.attribute_value_id,
                       coalesce(attr.attribute_custom_value, attr_v.attribute_value) as attribute_value
                from ref_cdb_attributes attr
                         left join ref_cdb_attribute_types attr_t on attr.attribute_type_id = attr_t.id
                         left join ref_cdb_attribute_values attr_v on attr.attribute_value_id = attr_v.id and
                                                                      attr.attribute_type_id = attr_v.attribute_type_id
                where getdate() >= attr.effective_from
                  and (attr.effective_to is null or attr.effective_to >= getdate())
            )
                  select u.id                                                     as user_id,
                         sc.external_id                                           as account_id,
                         addr.id                                                  as supply_address_id,
                         addr.postcode                                            as postcode,
                         fuel_types.fuel_type                                     as fuel_type,
                         nvl(ca_base_temp.attribute_value::double precision, -99) as base_temp,
                         20.0                                                     as default_base_temp,
                         nvl(ca_heating_basis.attribute_value, '')                as heating_basis,
                         nvl(ca_heating_controls.converted_value, ca_heating_control_type.attribute_value,
                             '')                                                  as heating_control_type,
                         nvl(ca_heating_source.attribute_value, '')               as heating_source,
                         nvl(ca_house_bedrooms.attribute_value, '')               as house_bedrooms,
                         nvl(ca_house_type.attribute_value, '')                   as house_type,
                         nvl(ca_house_age.attribute_value, '')                    as house_age,
                         nvl(ca_ages.attribute_value, '')                         as ages,
                         tado_heating_summary(nvl(ca_ages.attribute_value, ''))   as family_category,
                         case
                             when ca_base_temp.attribute_value is null and
                                  ca_heating_basis.attribute_value is null and
                                  ca_heating_controls.converted_value is null and
                                  ca_heating_source.attribute_value is null and
                                  ca_house_bedrooms.attribute_value is null and
                                  ca_house_type.attribute_value is null and
                                  ca_house_age.attribute_value is null and
                                  ca_ages.attribute_value is null
                                 then null
                             else u.id end                                        as sr_user_id
                  from ref_cdb_supply_contracts sc
                           inner join ref_cdb_user_permissions up
                                      on sc.id = up.permissionable_id and
                                         up.permissionable_type ilike 'app%supplycontract' and
                                         up.permission_level = 0
                           inner join ref_cdb_users u on up.user_id = u.id
                           inner join ref_cdb_addresses addr on addr.id = sc.supply_address_id
                           left join (select account_id, listagg(distinct meterpointtype) as fuel_type
                                      from ref_meterpoints
                                      group by account_id) fuel_types on fuel_types.account_id = sc.external_id
                           left join current_attributes ca_base_temp
                                     on ca_base_temp.attribute_name = 'temperature_preference' and
                                        ca_base_temp.user_id = u.id
                           left join current_attributes ca_heating_basis
                                     on ca_heating_basis.attribute_name = 'heating_basis' and
                                        ca_heating_basis.address_id = addr.id
                           left join (select attribute_name,
                                             address_id,
                                             attribute_value,
                                             case json_extract_path_text(attribute_value, 'type') + '~' +
                                                  json_extract_path_text(attribute_value, 'functionality')
                                                 when 'none~' then 'nocontrol'
                                                 when 'smart_thermostat~timer_control'
                                                     then 'smartthermostat'
                                                 when 'smart_thermostat~smart_app_control'
                                                     then 'smartthermostat'
                                                 when 'thermostat_only~thermostat_manual_control'
                                                     then 'thermostatmanual'
                                                 when 'thermostat_only~thermostat_set_control'
                                                     then 'thermostatautomatic'
                                                 when 'thermostat_only~manual_control'
                                                     then 'manually'
                                                 when 'timer_only~manual_control'
                                                     then 'manually'
                                                 when 'timer_only~timer_control'
                                                     then 'thermostatautomatic'
                                                 when 'timer_thermostat~thermostat_manual_control'
                                                     then 'thermostatmanual'
                                                 when 'timer_thermostat~thermostat_set_control'
                                                     then 'thermostatautomatic'
                                                 when 'timer_thermostat~manual_control'
                                                     then 'manually'
                                                 when 'timer_thermostat~timer_control'
                                                     then 'thermostatautomatic'
                                                 when 'unknown~' then 'unknown'
                                                 else null end as converted_value
                                      from current_attributes
                                      where attribute_name = 'heating_controls') ca_heating_controls
                                     on ca_heating_controls.attribute_name = 'heating_controls' and
                                        ca_heating_controls.address_id = addr.id
                           left join current_attributes ca_heating_control_type
                                     on ca_heating_control_type.attribute_name = 'heating_control_type' and
                                        ca_heating_control_type.address_id = addr.id
                           left join current_attributes ca_heating_source
                                     on ca_heating_source.attribute_name = 'heating_type' and
                                        ca_heating_source.address_id = addr.id
                           left join current_attributes ca_house_bedrooms
                                     on ca_house_bedrooms.attribute_name = 'house_bedrooms' and
                                        ca_house_bedrooms.address_id = addr.id
                           left join current_attributes ca_house_type
                                     on ca_house_type.attribute_name = 'house_type' and
                                        ca_house_type.address_id = addr.id
                           left join current_attributes ca_house_age
                                     on ca_house_age.attribute_name = 'house_age' and ca_house_age.address_id = addr.id
                           left join current_attributes ca_ages
                                     on ca_ages.attribute_name = 'resident_ages' and ca_ages.user_id = u.id) x
                     left outer join ref_consumption_accuracy_gas rcag
                                     on x.account_id = rcag.account_id -- moved to level 2 and removed group by
                     left outer join ref_tariff_history_gas_ur tf
                                     on tf.account_id = x.account_id and tf.end_date is null
                     left outer join ref_cdb_mmh_need_postcode_lookup mmhpc
                                     on mmhpc.outcode = left(x.postcode, len(postcode) - 3)
           ) x1
     ) x2