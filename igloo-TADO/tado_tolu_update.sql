--- TEMP TABLE ---
DROP TABLE IF EXISTS #temp_tado_eff;



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


into #temp_tado_eff ---- TEMP TABLE ---

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
                 --                else case
                 --                       when x1.aq != 0 then x1.aq
                 else case
                          when x1.gas_consumption_accuracy_value != 0 then x1.gas_consumption_accuracy_value
                     --                                   else case
                     --                                          when x1.gas_usage != 0 then x1.gas_usage
                          else (select gas_usage_gas_heating
                                from ref_cdb_mmh_need_profiling_lookup
                                where region_id = x1.region_id
                                  and prop_type = x1.mmhkw_prop_type_id
                                  and prop_age = x1.mmhkw_prop_age_id
                                  and floor_area_band = x1.mmhkw_floor_area_band) end end as annual_consumption,
             case
                 when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler')
                     or x1.heating_source = 'oilboiler' then 'mmh_kwh' --mmh_like_yours
             --                else case
             --                       when x1.aq != 0 then 'aq' -- aq
             --                       else case
             --                              when x1.gas_usage != 0 then 'quote' --quotes
             --                              else 'mmh_kwh'  --mmh_like_yours
             --                      end end end                                                                 as annual_consumption_source,
                 else case
                          when x1.gas_consumption_accuracy_value != 0 then x1.gas_consumption_accuracy_type -- aq
                     --                                   else case
                     --                                          when x1.gas_usage != 0 then 'quote' --quotes
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
                   tado_estimate_setpoint_impact(case
                                                     when x.base_temp <= 0 then x.default_base_temp
                                                     else x.base_temp end,
                                                 x.heating_control_type,
                                                 x.heating_basis)                           as estimated_temp,
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
                   --                                coalesce((select sum(reg.registers_eacaq)
                   --                                          from ref_meterpoints mp
                   --                                                 inner join ref_meters m
                   --                                                   on m.account_id = mp.account_id and m.meter_point_id = mp.meter_point_id and
                   --                                                      m.removeddate is null
                   --                                                 inner join ref_registers reg
                   --                                                   on reg.account_id = mp.account_id and reg.meter_id = m.meter_id
                   --                                          where mp.account_id = x.account_id
                   --                                            and mp.meterpointtype = 'G'
                   --                                            and (mp.supplyenddate is null or mp.supplyenddate >= getdate())
                   --                                          group by mp.account_id), 0)                                    as aq,
                   --                                coalesce((select q.gas_usage
                   --                                          from ref_cdb_registrations r
                   --                                                 inner join ref_cdb_quotes q on r.quote_id = q.id
                   --                                          where r.id = x.account_id
                   --                                          order by q.updated_at desc
                   --                                          limit 1), 0)                                                   as gas_usage,
                   get_best_consumption(coalesce(rcag.igl_ind_aq, 0), coalesce(rcag.ind_aq, 0),
                                        coalesce(rcag.pa_cons_gas, 0), coalesce(rcag.quotes_aq, 0),
                                        'gas')                                              as gas_consumption_accuracy_type,
                   case
                       when get_best_consumption(coalesce(rcag.igl_ind_aq, 0), coalesce(rcag.ind_aq, 0),
                                                 coalesce(rcag.pa_cons_gas, 0), coalesce(rcag.quotes_aq, 0),
                                                 'gas') = 'igl_ind_aq'
                           then rcag.igl_ind_aq
                       when get_best_consumption(coalesce(rcag.igl_ind_aq, 0), coalesce(rcag.ind_aq, 0),
                                                 coalesce(rcag.pa_cons_gas, 0), coalesce(rcag.quotes_aq, 0),
                                                 'gas') = 'ind_aq'
                           then rcag.ind_aq
                       when get_best_consumption(coalesce(rcag.igl_ind_aq, 0), coalesce(rcag.ind_aq, 0),
                                                 coalesce(rcag.pa_cons_gas, 0), coalesce(rcag.quotes_aq, 0),
                                                 'gas') = 'pa_cons_gas'
                           then rcag.pa_cons_gas
                       when get_best_consumption(coalesce(rcag.igl_ind_aq, 0), coalesce(rcag.ind_aq, 0),
                                                 coalesce(rcag.pa_cons_gas, 0), coalesce(rcag.quotes_aq, 0),
                                                 'gas') = 'quotes_aq'
                           then rcag.quotes_aq
                       else 0 end                                                           as gas_consumption_accuracy_value


                   -- Model 1st Level Inputs
            from (select user_id,
                         account_id,
                         supply_address_id,
                         postcode,

                         max(sr_user_id)                           as sr_user_id,
                         cast(max(base_temp) as double precision)  as base_temp,
                         20.00                                     as default_base_temp,


                         max(heating_basis)                        as heating_basis,
                         max(heating_control_type)                 as heating_control_type,
                         max(heating_source)                       as heating_source,
                         max(house_bedrooms)                       as house_bedrooms,
                         max(house_type)                           as house_type,
                         max(house_age)                            as house_age,


                         (select listagg(distinct mp.meterpointtype)
                          from ref_meterpoints mp
                          where mp.account_id = l1.account_id ---- su.external_id
                                --and (mp.supplyenddate is null or mp.supplyenddate >= getdate())
                          group by mp.account_id)                  as fuel_type,
                         coalesce(max(attribute_custom_value), '') as ages,
                         max(family_category)                      as family_category


                  from
                      ----- New SUB-QUERY : Added: T.A ; Purpose: Improve Query Performance ;  Date: 15/11/2019 -----
                      (
                          select u.id                      as user_id,
                                 su.external_id            as account_id,
                                 su.supply_address_id      as supply_address_id,
                                 addr.postcode             as postcode,
                                 sr.user_id                as sr_user_id,
                                 at.attribute_custom_value as attribute_custom_value,

                                 case
                                     when att.attribute_name = 'temperature_preference' then av.attribute_value
                                     else '-99' end        as base_temp,

                                 case
                                     when att.attribute_name = 'heating_basis' then av.attribute_value
                                     else '' end           as heating_basis,
                                 case
                                     when att.attribute_name = 'heating_control_type' then av.attribute_value
                                     else '' end           as heating_control_type,
                                 case
                                     when att.attribute_name = 'heating_type' then av.attribute_value
                                     else '' end           as heating_source,
                                 case
                                     when att.attribute_name = 'house_bedrooms' then av.attribute_value
                                     else '' end           as house_bedrooms,
                                 case
                                     when att.attribute_name = 'house_type' then av.attribute_value
                                     else '' end           as house_type,
                                 case
                                     when att.attribute_name = 'house_age' then av.attribute_value
                                     else '' end           as house_age,
                                 case
                                     when att.attribute_name = 'resident_ages'
                                         then tado_heating_summary(coalesce(at.attribute_custom_value, ''))
                                     else '' end           as family_category

                          from ref_cdb_supply_contracts su
                                   inner join ref_cdb_addresses addr on su.supply_address_id = addr.id
                                   inner join ref_cdb_user_permissions up
                                              on su.id = up.permissionable_id and permission_level = 0
                                                  and permissionable_type ILIKE 'App%SupplyContract'
                                   inner join ref_cdb_users u on u.id = up.user_id
                                   left outer join ref_cdb_attributes at on (
                                                                                    (at.entity_id = up.user_id AND at.entity_type ILIKE 'App%User')
                                                                                    OR
                                                                                    (at.entity_id = su.supply_address_id AND
                                                                                     at.entity_type ILIKE 'App%Address')
                                                                                )
                              and at.effective_to is null

                                   left outer join ref_cdb_attribute_types att
                                                   on att.id = at.attribute_type_id and att.effective_to is null
                                   left outer join ref_cdb_attribute_values av
                                                   on av.attribute_type_id = at.attribute_type_id and
                                                      at.attribute_value_id = av.id
                                                       and at.attribute_value_id is not null
                                   left outer join ref_cdb_survey_questions sq on sq.attribute_type_id = att.id
                                   left outer join ref_cdb_survey_category sc on sc.id = sq.survey_category_id
                                   left outer join (select user_id, survey_id
                                                    from ref_cdb_survey_response
                                                    where survey_id = 1
                                                    group by user_id, survey_id) sr on sr.user_id = up.user_id
                          where (att.attribute_name in ('resident_ages',
                                                        'heating_control_type',
                                                        'temperature_preference',
                                                        'heating_basis',
                                                        'heating_type',
                                                        'house_bedrooms',
                                                        'house_type',
                                                        'house_age')
                              or sr.user_id is null or att.attribute_name is null)
                      ) l1
                  group by user_id, account_id, supply_address_id, postcode) x
                     left outer join ref_consumption_accuracy_gas rcag
                                     on x.account_id = rcag.account_id -- moved to level 2 and removed group by
                     left outer join ref_tariff_history_gas_ur tf
                                     on tf.account_id = x.account_id and tf.end_date is null
                     left outer join ref_cdb_mmh_need_postcode_lookup mmhpc
                                     on mmhpc.outcode = left(x.postcode, len(postcode) - 3)
           ) x1
     ) x2;



------------------------------------------------------------------------------
------------------------------------------------------------------------------
------------------------ ANALYSIS --------------------------------------------
------------------------------------------------------------------------------
------------------------------------------------------------------------------

select *
from #temp_tado_eff
where user_id = 57676
limit 10;
