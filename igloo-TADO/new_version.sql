drop table temp_tado_new_style;
create table temp_tado_new_style as
with current_attributes as (
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
select u.id                                                               as user_id,
       sc.external_id                                                     as account_id,
       addr.id                                                            as supply_address_id,
       addr.postcode                                                      as postcode,
       case
           when len(fuel_types.fuel_type) = 2 then 'Dual'
           else fuel_types.fuel_type end                                  as fuel_type,
       ca_base_temp.attribute_value::double precision                     as base_temp,
       nvl(ca_heating_basis.attribute_value, 'unknown')                   as heating_basis,
       nvl(ca_heating_controls.converted_value, ca_heating_control_type.attribute_value,
           'unknown')                                                     as heating_control_type,
       nvl(ca_heating_source.attribute_value, 'unknown')                  as heating_source,
       nvl(ca_house_bedrooms.attribute_value, 'unknown')                  as house_bedrooms,
       nvl(ca_house_type.attribute_value, 'unknown')                      as house_type,
       nvl(ca_house_age.attribute_value, 'unknown')                       as house_age,
       nvl(ca_ages.attribute_value, 'unknown')                            as ages,
       tado_heating_summary(nvl(ca_ages.attribute_value, ''))             as family_category,
       case
           when surv_resp.user_id is null
               then 'nodata'
           when base_temp <= 0 or
                heating_control_type in ('', 'unknown') or
                heating_basis in ('', 'unknown') or
                family_category in ('', 'unknown') or
                heating_source in ('', 'unknown') or
                ages in ('', 'unknown')
               then 'incomplete'
           else 'complete' end                                            as mmh_tado_status,
       nvl(ca_base_temp.attribute_value::double precision, 20.0)          as base_temp_used,
       tado_estimate_setpoint_impact(base_temp_used, heating_control_type,
                                     heating_basis)                       as estimated_temp,
       tado_estimate_heating_hours(family_category, 'base')               as base_hours,
       tado_estimate_heating_hours(family_category, 'estimate')           as estimated_hours,
       tado_estimate_mean_internal_temp(base_temp_used,
                                        nvl(base_hours, 0.0))             as base_mit,
       tado_estimate_mean_internal_temp(estimated_temp,
                                        nvl(estimated_hours, 0.0))        as estimated_mit,
       tado_get_heating_source(heating_source)                            as mmhkw_heating_source,
       tado_get_prop_floor_area_band(house_bedrooms, house_type)          as mmhkw_floor_area_band,
       tado_get_prop_age_id(house_age)                                    as mmhkw_prop_age_id,
       tado_get_prop_type_id(house_type)                                  as mmhkw_prop_type_id,
       mmhpc.region_id                                                    as region_id,
       case
           when (fuel_type = 'E' and heating_source = 'gasboiler')
               or heating_source = 'oilboiler' then (select gas_usage_gas_heating
                                                     from ref_cdb_mmh_need_profiling_lookup
                                                     where region_id = mmhpc.region_id
                                                       and prop_type = mmhkw_prop_type_id
                                                       and prop_age = mmhkw_prop_age_id
                                                       and floor_area_band = mmhkw_floor_area_band)
           when nvl(cag.ca_value, 0) != 0 then cag.ca_value
           else (select gas_usage_gas_heating
                 from ref_cdb_mmh_need_profiling_lookup
                 where region_id = mmhpc.region_id
                   and prop_type = mmhkw_prop_type_id
                   and prop_age = mmhkw_prop_age_id
                   and floor_area_band = mmhkw_floor_area_band) end       as annual_consumption,
       case
           when (fuel_type = 'E' and heating_source = 'gasboiler')
               or heating_source = 'oilboiler' then 'mmh_kwh' --mmh_like_yours
           when nvl(cag.ca_value, 0) != 0 then cag.ca_source -- aq
           else 'mmh_kwh' --mmh_like_yours
           end                                                            as annual_consumption_source,
       case
           when (fuel_type = 'E' and heating_source = 'gasboiler')
               then (gas_off_grid.fuel_rate + (gas_off_grid.fuel_rate * .05)) / 100
           when heating_source = 'oilboiler'
               then (oil_off_grid.fuel_rate + (oil_off_grid.fuel_rate * .05)) / 100
           else (gas_grid_ur.rate + (gas_grid_ur.rate * .05)) / 100 end   as unit_rate_with_vat,
       case
           when (fuel_type = 'E' and heating_source = 'gasboiler') then 'gas_offgrid'
           when heating_source = 'oilboiler' then 'oil'
           else 'gas' end                                                 as unit_rate_source,
       ((estimated_mit - base_mit) / base_mit * 100)                      as savings_perc,
       (select avg_perc_diff from ref_calculated_tado_efficiency_average) as avg_savings_perc,
       case
           when mmh_tado_status in ('nodata', 'incomplete') then 'avg_perc'
           else 'tado_perc'
           end                                                            as savings_perc_source,
       annual_consumption * unit_rate_with_vat                            as amount_over_year,
       case
           when savings_perc_source = 'avg_perc' then
               (annual_consumption * unit_rate_with_vat * avg_savings_perc) / 100
           else (annual_consumption * unit_rate_with_vat * savings_perc) / 100
           end                                                            as savings_in_pounds,
       tado_savings_segmentation(mmh_tado_status,
                                 fuel_type,
                                 heating_control_type,
                                 heating_source,
                                 savings_in_pounds)                       as segment,
       getdate()                                                          as etlchange
from ref_cdb_supply_contracts sc
         inner join ref_cdb_user_permissions up
                    on sc.id = up.permissionable_id and up.permissionable_type ilike 'app%supplycontract' and
                       up.permission_level = 0
         inner join ref_cdb_users u on up.user_id = u.id
         inner join ref_cdb_addresses addr on addr.id = sc.supply_address_id
         left join (select account_id, listagg(distinct meterpointtype) as fuel_type
                    from ref_meterpoints
                    group by account_id) fuel_types on fuel_types.account_id = sc.external_id
         left join current_attributes ca_base_temp
                   on ca_base_temp.attribute_name = 'temperature_preference' and ca_base_temp.user_id = u.id
         left join current_attributes ca_heating_basis
                   on ca_heating_basis.attribute_name = 'heating_basis' and ca_heating_basis.address_id = addr.id
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
                   on ca_heating_source.attribute_name = 'heating_type' and ca_heating_source.address_id = addr.id
         left join current_attributes ca_house_bedrooms
                   on ca_house_bedrooms.attribute_name = 'house_bedrooms' and ca_house_bedrooms.address_id = addr.id
         left join current_attributes ca_house_type
                   on ca_house_type.attribute_name = 'house_type' and ca_house_type.address_id = addr.id
         left join current_attributes ca_house_age
                   on ca_house_age.attribute_name = 'house_age' and ca_house_age.address_id = addr.id
         left join current_attributes ca_ages on ca_ages.attribute_name = 'resident_ages' and ca_ages.user_id = u.id
         left join ref_cdb_mmh_need_postcode_lookup mmhpc on mmhpc.outcode = left(addr.postcode, len(addr.postcode) - 3)
         left join vw_cons_acc_gas_all cag on cag.account_id = sc.external_id
         left join ref_calculated_tado_fuel gas_off_grid on gas_off_grid.fuel_tariff_name = 'gas'
         left join ref_calculated_tado_fuel oil_off_grid on oil_off_grid.fuel_tariff_name = 'oil'
         left join ref_tariff_history_gas_ur gas_grid_ur
                   on gas_grid_ur.account_id = sc.external_id and gas_grid_ur.end_date is null
         left join (select distinct user_id from ref_cdb_survey_response where survey_id = 1) surv_resp
                   on surv_resp.user_id = u.id

-- select pid, user_name, starttime, query
-- from stv_recents
-- where status='Running';
--
-- cancel 10540 'runaway sql'

with current_attributes as (
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
select u.id                                                                                  as user_id,
       sc.external_id                                                                        as account_id,
       addr.id                                                                               as supply_address_id,
       addr.postcode                                                                         as postcode,
       fuel_types.fuel_type                                                                  as fuel_type,
       nvl(ca_base_temp.attribute_value::double precision, -99)                              as base_temp,
       20.0                                                                                  as default_base_temp,
       nvl(ca_heating_basis.attribute_value, '')                                             as heating_basis,
       nvl(ca_heating_controls.converted_value, ca_heating_control_type.attribute_value, '') as heating_control_type,
       nvl(ca_heating_source.attribute_value, '')                                            as heating_source,
       nvl(ca_house_bedrooms.attribute_value, '')                                            as house_bedrooms,
       nvl(ca_house_type.attribute_value, '')                                                as house_type,
       nvl(ca_house_age.attribute_value, '')                                                 as house_age,
       nvl(ca_ages.attribute_value, '')                                                      as ages,
      tado_heating_summary(nvl(ca_ages.attribute_value, ''))                                        as family_category,
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
           else u.id end                                                                     as sr_user_id
from ref_cdb_supply_contracts sc
         inner join ref_cdb_user_permissions up
                    on sc.id = up.permissionable_id and up.permissionable_type ilike 'app%supplycontract' and
                       up.permission_level = 0
         inner join ref_cdb_users u on up.user_id = u.id
         inner join ref_cdb_addresses addr on addr.id = sc.supply_address_id
         left join (select account_id, listagg(distinct meterpointtype) as fuel_type
                    from ref_meterpoints
                    group by account_id) fuel_types on fuel_types.account_id = sc.external_id
         left join current_attributes ca_base_temp
                   on ca_base_temp.attribute_name = 'temperature_preference' and ca_base_temp.user_id = u.id
         left join current_attributes ca_heating_basis
                   on ca_heating_basis.attribute_name = 'heating_basis' and ca_heating_basis.address_id = addr.id
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
                   on ca_heating_source.attribute_name = 'heating_type' and ca_heating_source.address_id = addr.id
         left join current_attributes ca_house_bedrooms
                   on ca_house_bedrooms.attribute_name = 'house_bedrooms' and ca_house_bedrooms.address_id = addr.id
         left join current_attributes ca_house_type
                   on ca_house_type.attribute_name = 'house_type' and ca_house_type.address_id = addr.id
         left join current_attributes ca_house_age
                   on ca_house_age.attribute_name = 'house_age' and ca_house_age.address_id = addr.id
         left join current_attributes ca_ages on ca_ages.attribute_name = 'resident_ages' and ca_ages.user_id = u.id
