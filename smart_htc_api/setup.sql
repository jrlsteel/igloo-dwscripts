select sc.external_id,
       addr.id              as my_reference,
       pc.latitude,
       pc.longitude,
       addr.postcode,
       epc.total_floor_area as floor_area,
       case
           when epc.property_type is not null then
               case
                   when epc.property_type in ('Flat', 'Maisonette') then
                       case
                           when epc.floor_level ilike 'ground%' then
                               'ground floor flat'
                           when epc.floor_level ilike 'top floor' or epc.flat_top_storey = 'Y' then
                               'top floor flat'
                           else
                               'mid floor flat'
                           end
                   else case epc.built_form
                            when 'Enclosed End-Terrace' then 'End terrace'
                            when 'End-Terrace' then 'End terrace'
                            when 'Enclosed Mid-Terrace' then 'Mid terrace'
                            when 'Mid-Terrace' then 'Mid terrace'
                            when 'Detached' then 'Detached'
                            when 'Semi-Detached' then 'Semi-detached'
                       end
                   end
           else
               case p_type.property_type_id
                   when 1 then 'Detached'
                   when 2 then 'Semi-detached'
                   when 3 then 'End terrace'
                   when 4 then 'Mid terrace'
                   when 5 then 'Detached'
                   when 6 then 'mid floor flat'
                   when 7 then 'mid floor flat'
                   end
           end              as attachment,
       case
           when nvl(epc.wind_turbine_count, 0) > 0 or
                nvl(epc.solar_water_heating_flag, 'N') = 'Y' or
                nvl(epc.photo_supply, 0) > 0 then 'true'
           else 'false' end as renewables,
       case
           when mmh_fuel_type.fuel is not null then mmh_fuel_type.fuel
           when q.heating_type in ('gas', 'electricity', 'other') then q.heating_type
           else null
           end              as heating_type
from ref_cdb_supply_contracts sc
         inner join ref_cdb_addresses addr on sc.supply_address_id = addr.id
         left join ref_postcodes pc on replace(pc.postcode, ' ', '') = replace(addr.postcode, ' ', '')
         left join ref_cdb_registrations r on r.id = sc.registration_id
         left join ref_cdb_quotes q on r.quote_id = q.id
         left join (select entity_id as supply_address_id,
                           case min(case attribute_value_id
                                        when 26 then 1
                                        when 27 then 3
                                        when 28 then 3
                                        when 29 then 2
                                        when 30 then 2
                                        when 31 then 3
                                        when 32 then 2
                                        when 33 then 1
                               end)
                               when 1 then 'gas'
                               when 2 then 'elec'
                               when 3 then 'other'
                               end   as fuel
                    from ref_cdb_attributes
                    where attribute_type_id = 5
                      and (effective_to is null or effective_to > getdate())
                    group by entity_id) mmh_fuel_type on mmh_fuel_type.supply_address_id = sc.supply_address_id
         left join ref_epc_certificates epc on
            regexp_replace(lower(nvl(addr.sub_building_name_number, '') +
                                 nvl(addr.building_name_number, '') +
                                 nvl(addr.thoroughfare, '') +
                                 nvl(addr.dependent_locality, '')), '[^a-z0-9]', '') =
            regexp_replace(lower(epc.address), '[^a-z0-9]', '') and
            replace(addr.postcode, ' ', '') = replace(epc.postcode, ' ', '')
         left join (select entity_id as address_id, max(attribute_value_id) property_type_id
                    from ref_cdb_attributes
                    where attribute_type_id = 2
                    group by entity_id) p_type on p_type.address_id = sc.supply_address_id