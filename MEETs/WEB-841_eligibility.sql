-- has dcc enabled gas smart meter
-- has dcc enabled elec smart meter
-- MMH completion?
-- HH consent
--
--
-- truncate table ref_meets_eligibility;
-- insert into ref_meets_eligibility
select num_smart.account_id,
       num_s2_elec,
       num_s2_gas,
       -- nvl(mmh_subset_complete, false) as mmh_subset_complete,
       nvl(attr_consent.attribute_value_id, 0) = 132 as hh_consent,
       account_status
from (select cf.account_id,
             cf.account_status,
             sum((rm.meterpointtype = 'E' and
                  rma_metertype.metersattributes_attributevalue ilike 'S2%')::int)                        as num_s2_elec,
             sum((rm.meterpointtype = 'G' and rma_mech.metersattributes_attributevalue ilike 'S2%')::int) as num_s2_gas
      from ref_calculated_daily_customer_file cf
               left join ref_meterpoints rm on cf.account_id = rm.account_id
               left join ref_meters met on rm.account_id = met.account_id and rm.meter_point_id = met.meter_point_id and
                                           met.removeddate is null
               left join ref_meters_attributes rma_metertype
                         on rma_metertype.metersattributes_attributename = 'MeterType' and
                            met.account_id = rma_metertype.account_id and
                            met.meter_point_id = rma_metertype.meter_point_id and
                            met.meter_id = rma_metertype.meter_id
               left join ref_meters_attributes rma_mech
                         on rma_mech.metersattributes_attributename = 'Meter_Mechanism_Code' and
                            met.account_id = rma_mech.account_id and met.meter_point_id = rma_mech.meter_point_id and
                            met.meter_id = rma_mech.meter_id
      group by cf.account_id, cf.account_status) num_smart
         /*left join (select sc.external_id                             as account_id,
                           count(distinct attr.attribute_type_id) = 6 as mmh_subset_complete
                    from ref_cdb_supply_contracts sc
                             left join ref_cdb_addresses addr on sc.supply_address_id = addr.id
                             left join ref_cdb_user_permissions up
                                       on up.permissionable_type ilike 'app%supplycontract' and
                                          up.permissionable_id = sc.id
                             left join ref_cdb_users u on u.id = up.user_id
                             left join ref_cdb_attributes attr
                                       on attr.effective_from <= getdate() and
                                          (attr.effective_to is null or attr.effective_to >= getdate()) and
                                          ((attr.attribute_type_id = 1 and attr.entity_id = u.id)
                                              or
                                           (attr.attribute_type_id in (2, 3, 4, 5, 8) and attr.entity_id = addr.id))
                    group by account_id) mmh_completion on num_smart.account_id = mmh_completion.account_id*/
         left join ref_cdb_supply_contracts sc on sc.external_id = num_smart.account_id
         left join ref_cdb_attributes attr_consent
                   on attr_consent.attribute_type_id = 23 and attr_consent.entity_id = sc.id
order by account_id