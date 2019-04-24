--Batch SQL for igloo-TADO
--Model Outputs and Calculations
drop table ref_calculated_tado_efficiency_batch;

-- insert into ref_calculated_tado_efficiency_batch
-- (
select
x2.user_id,
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
   when x2.mmh_tado_status in ('nodata', 'incomplete') then
     'avg_perc'
   else 'tado_perc'
     end                                                                                        as savings_perc_source,

x2.annual_consumption * x2.unit_rate_with_vat                                                   as amount_over_year,

case
   when x2.mmh_tado_status in ('nodata', 'incomplete') then
     (x2.annual_consumption * x2.unit_rate_with_vat * x2.avg_savings_perc) / 100
   else
     (x2.annual_consumption * x2.unit_rate_with_vat * x2.savings_perc) / 100
     end                                                                                        as savings_in_pounds,
 tado_savings_segmentation(
   x2.mmh_tado_status,
   x2.fuel_type,
   x2.heating_control_type,
   x2.heating_source,
  (case
   when x2.mmh_tado_status in ('nodata', 'incomplete') then
     (x2.annual_consumption * x2.unit_rate_with_vat * x2.avg_savings_perc) / 100
   else
     (x2.annual_consumption * x2.unit_rate_with_vat * x2.savings_perc) / 100
     end
   ))                                                                                            as segment,
       getdate()                                                                                as etlchange
      --Model 3rd Level Inputs
from (select x1.*,
             tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0),
                                              coalesce(x1.base_hours, 0.0))                      as base_mit,
             tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0),
                                              coalesce(x1.estimated_hours, 0.0))                 as estimated_mit,
             (
                 (
                     tado_estimate_mean_internal_temp(coalesce(x1.estimated_temp, 0.0),
                                                      coalesce(x1.estimated_hours, 0.0)) -
                     tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0), coalesce(x1.base_hours, 0.0))
                     ) /
                 tado_estimate_mean_internal_temp(coalesce(x1.base_temp_used, 0.0), coalesce(x1.base_hours, 0.0)) * 100
                 )                                                                               as savings_perc,
             (select avg_perc_diff
              from ref_calculated_tado_efficiency_average)                                       as avg_savings_perc,
             case
               when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler')
                      or x1.heating_source = 'oilboiler' then (select gas_usage_gas_heating
                                                               from ref_cdb_mmh_need_profiling_lookup
                                                               where region_id = x1.region_id
                                                                 and prop_type = x1.mmhkw_prop_type_id
                                                                 and prop_age = x1.mmhkw_prop_age_id
                                                                 and floor_area_band = x1.mmhkw_floor_area_band)
               else case
                      when x1.aq != 0 then x1.aq
                      else case
                             when x1.gas_usage != 0 then x1.gas_usage
                             else (select gas_usage_gas_heating
                                   from ref_cdb_mmh_need_profiling_lookup
                                   where region_id = x1.region_id
                                     and prop_type = x1.mmhkw_prop_type_id
                                     and prop_age = x1.mmhkw_prop_age_id
                                     and floor_area_band = x1.mmhkw_floor_area_band) end end end as annual_consumption,
             case
               when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler')
                      or x1.heating_source = 'oilboiler' then 'mmh_kwh' --mmh_like_yours
               else case
                      when x1.aq != 0 then 'aq' -- aq
                      else case
                             when x1.gas_usage != 0 then 'quote' --quotes
                             else 'mmh_kwh'  --mmh_like_yours
                     end end end                                                                 as annual_consumption_source,
             case
               when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler')
                       then (unit_rate_gas_offgrid + (unit_rate_gas_offgrid * .05)) / 100
               else case
                      when x1.heating_source = 'oilboiler'
                              then (unit_rate_oil + (unit_rate_oil * .05)) / 100
                      else (unit_rate_gas + (unit_rate_gas * .05)) / 100 end end                 as unit_rate_with_vat,
             case
               when (x1.fuel_type = 'E' and x1.heating_source = 'gasboiler') then 'gas_offgrid'
               else case
                      when x1.heating_source = 'oilboiler' then 'oil'
                      else 'gas' end end                                                         as unit_rate_source
      --Model 2nd Level Inputs
      from (select x.user_id,
                   x.account_id,
                   x.supply_address_id,
                   x.postcode,
                   x.base_temp,
                   case
                     when x.base_temp <= 0 then x.default_base_temp
                     else x.base_temp end                                                   as base_temp_used,
                   case
                     when heating_control_type = '' then 'unknown'
                     else x.heating_control_type end                                        as heating_control_type,
                   case when x.heating_basis = '' then 'unknown' else x.heating_basis end   as heating_basis,
                   case
                     when x.family_category = '' or x.family_category='unknown' then 'unknown'
                     else x.family_category end                                             as family_category,
                   case when x.heating_source = '' then 'unknown' else x.heating_source end as heating_source,
                   case when x.house_bedrooms = '' then 'unknown' else x.house_bedrooms end as house_bedrooms,
                   case when x.house_type = '' then 'unknown' else x.house_type end as house_type,
                   case when x.house_age = '' then 'unknown' else x.house_age end as house_age,

                   case when x.ages = '' then 'unknown' else x.ages end                     as ages,
                   (case
                      when x.sr_user_id is null
                              then 'nodata'
                      else case
                             when x.base_temp <= 0 or heating_control_type = '' or x.heating_basis = '' or
                                  x.family_category in ('','unknown') or
                                  x.heating_source = '' or x.ages = ''
                                     then 'incomplete'
                             else 'complete' end end)                                       as mmh_tado_status,
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
                   coalesce((select sum(reg.registers_eacaq)
                             from ref_meterpoints mp
                                    inner join ref_meters m
                                      on m.account_id = mp.account_id and m.meter_point_id = mp.meter_point_id and
                                         m.removeddate is null
                                    inner join ref_registers reg
                                      on reg.account_id = mp.account_id and reg.meter_id = m.meter_id
                             where mp.account_id = x.account_id
                               and mp.meterpointtype = 'G'
                               and (mp.supplyenddate is null or mp.supplyenddate >= getdate())
                             group by mp.account_id), 0)                                    as aq,
                   coalesce((select q.gas_usage
                             from ref_cdb_registrations r
                                    inner join ref_cdb_quotes q on r.quote_id = q.id
                             where r.id = x.account_id
                             order by q.updated_at desc
                             limit 1), 0)                                                   as gas_usage
      -- Model 1st Level Inputs
            from (select u.id                                            as user_id,
                         su.external_id                                  as account_id,
                         su.supply_address_id                            as supply_address_id,
                         addr.postcode                                   as postcode,
                         max(sr.user_id)                                 as sr_user_id,
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
                         max(case
                               when att.attribute_name = 'house_bedrooms' then av.attribute_value
                               else '' end)                              as house_bedrooms,
                         max(case
                               when att.attribute_name = 'house_type' then av.attribute_value
                               else '' end)                              as house_type,
                         max(case
                               when att.attribute_name = 'house_age' then av.attribute_value
                               else '' end)                              as house_age,
                         (select listagg(distinct mp.meterpointtype)
                          from ref_meterpoints mp
                          where account_id = su.external_id
                            and (mp.supplyenddate is null or mp.supplyenddate >= getdate())
                          group by account_id)                           as fuel_type,
                         coalesce(max(at.attribute_custom_value), '')    as ages,
                         max(case
                               when att.attribute_name = 'resident_ages'
                                       then tado_heating_summary(coalesce(at.attribute_custom_value, ''))
                               else '' end)                              as family_category
                  from ref_cdb_supply_contracts su
                         inner join ref_cdb_addresses addr on su.supply_address_id = addr.id
                         inner join ref_cdb_user_permissions up on su.id = up.permissionable_id and permission_level = 0
                                                                     and permissionable_type = 'App\\SupplyContract'
                         inner join ref_cdb_users u on u.id = up.user_id
                         left outer join ref_cdb_attributes at on (
                                                                      (at.entity_id = up.user_id AND at.entity_type = 'App\\User')
                                                                        OR
                                                                      (at.entity_id = su.supply_address_id AND at.entity_type = 'App\\Address')
                                                                      )
                                                                    and at.effective_to is null
                         left outer join ref_cdb_attribute_types att
                           on att.id = at.attribute_type_id and att.effective_to is null
                         left outer join ref_cdb_attribute_values av
                           on av.attribute_type_id = at.attribute_type_id and at.attribute_value_id = av.id
                                and at.attribute_value_id is not null
                         left outer join ref_cdb_survey_questions sq on sq.attribute_type_id = att.id
                         left outer join ref_cdb_survey_category sc on sc.id = sq.survey_category_id
                         left outer join (select user_id, survey_id
                                          from ref_cdb_survey_response
                                          where survey_id = 1
                                          group by user_id, survey_id) sr on sr.user_id = up.user_id
                  where
                      su.external_id = 1832 and
                     (att.attribute_name in ('resident_ages',
                                                'heating_control_type',
                                                'temperature_preference',
                                                'heating_basis',
                                                'heating_type',
                                                'house_bedrooms',
                                                'house_type',
                                                'house_age')
                           or sr.user_id is null)
                  group by u.id, su.external_id, su.supply_address_id, addr.postcode) x
                   left outer join ref_tariff_history_gas_ur tf on tf.account_id = x.account_id and tf.end_date is null
                   left outer join ref_cdb_mmh_need_postcode_lookup mmhpc
                     on mmhpc.outcode = left(x.postcode, len(postcode) - 3)) x1) x2
-- )
;

drop table ref_calculated_tado_efficiency_batch;

create table ref_calculated_tado_efficiency_batch
(
	user_id bigint,
	account_id bigint distkey,
	supply_address_id bigint,
	postcode varchar(255),
	fuel_type varchar(65535),
	base_temp double precision,
	heating_basis varchar(255),
	heating_control_type varchar(255),
	heating_source varchar(255),
	house_bedrooms varchar(255),
	house_type varchar(255),
	house_age varchar(255),
	ages varchar(65535),
	family_category varchar(500),
	mmh_tado_status varchar(255),
	base_temp_used double precision,
	estimated_temp double precision,
	base_hours double precision,
	estimated_hours double precision,
	base_mit double precision,
	estimated_mit double precision,
	mmhkw_heating_source varchar(255),
	mmhkw_floor_area_band integer,
	mmhkw_prop_age_id integer,
	mmhkw_prop_type_id integer,
	region_id integer,
	annual_consumption double precision,
	annual_consumption_source varchar(255),
	unit_rate_with_vat double precision,
	unit_rate_source varchar(255),
	savings_perc double precision,
	avg_savings_perc double precision,
	savings_perc_source varchar(255),
	amount_over_year double precision,
	savings_in_pounds double precision,
	segment integer,
	etlchange timestamp
)
diststyle key
;

alter table ref_calculated_tado_efficiency_batch owner to igloo
;

drop table ref_calculated_tado_efficiency_audit;

create table ref_calculated_tado_efficiency_audit
(
	user_id bigint,
	account_id bigint distkey,
	supply_address_id bigint,
	postcode varchar(255),
	fuel_type varchar(65535),
	base_temp double precision,
	heating_basis varchar(255),
	heating_control_type varchar(255),
	heating_source varchar(255),
	house_bedrooms varchar(255),
	house_type varchar(255),
	house_age varchar(255),
	ages varchar(65535),
	family_category varchar(500),
	mmh_tado_status varchar(255),
	base_temp_used double precision,
	estimated_temp double precision,
	base_hours double precision,
	estimated_hours double precision,
	base_mit double precision,
	estimated_mit double precision,
	mmhkw_heating_source varchar(255),
	mmhkw_floor_area_band integer,
	mmhkw_prop_age_id integer,
	mmhkw_prop_type_id integer,
	region_id integer,
	annual_consumption double precision,
	annual_consumption_source varchar(255),
	unit_rate_with_vat double precision,
	unit_rate_source varchar(255),
	savings_perc double precision,
	avg_savings_perc double precision,
	savings_perc_source varchar(255),
	amount_over_year double precision,
	savings_in_pounds double precision,
	segment integer,
	etlchange timestamp
)
diststyle key
;
