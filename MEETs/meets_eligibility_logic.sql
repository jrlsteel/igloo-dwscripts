-- has dcc enabled gas smart meter
-- has dcc enabled elec smart meter
-- MMH completion?
-- HH consent
--
--
truncate table ref_meets_eligibility;
insert into ref_meets_eligibility
select num_smart.account_id,
       num_s2_elec,
       num_s2_gas,
       -- nvl(mmh_subset_complete, false) as mmh_subset_complete,
       nvl(attr_consent.attribute_value_id, 0) = 132 as hh_consent,
       account_status,
       getdate()                                     as etlchange
from (select cf.account_id,
             cf.account_status,
             nvl(sum((rm.meterpointtype = 'E' and
                      rma_metertype.metersattributes_attributevalue ilike 'S2%')::int), 0) as num_s2_elec,
             nvl(sum((rm.meterpointtype = 'G' and rma_mech.metersattributes_attributevalue ilike 'S2%')::int),
                 0)                                                                        as num_s2_gas
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
         left join ref_cdb_supply_contracts sc on sc.external_id = num_smart.account_id
         left join ref_cdb_attributes attr_consent
                   on attr_consent.attribute_type_id = 23 and attr_consent.entity_id = sc.id
order by account_id


-- stats for each live meter
-- create table temp_meter_info as
truncate table temp_meter_info
insert into temp_meter_info
with cte_meters_attr as
         (select account_id,
                 meter_point_id,
                 meter_id,
                 metersattributes_attributename       as name,
                 max(metersattributes_attributevalue) as value
          from ref_meters_attributes
          group by account_id, meter_point_id, meter_id, metersattributes_attributename),
     cte_device_id_map as
         (select distinct upper(dspinventory_esme_deviceid) as device_id,
                          dspinventory_esme_importmpxn      as mpxn,
                          'E'                               as fuel
          from ref_smart_inventory
          union
          select distinct upper(dspinventory_gpf_deviceid) as deviceid,
                          dspinventory_gsme_importmpxn     as mpxn,
                          'G'                              as fuel
          from ref_smart_inventory)
select mp.account_id,
       mp.meter_point_id,
       met.meter_id,
       mp.meterpointtype,
       left(nvl(gas_met_type.value, elec_met_type.value), 2) = 'S2' as is_S2_meter,
       dev_ids.device_id is not null                                as is_dcc_enabled,
       dev_ids.device_id,
       hh_read_device_ids.deviceid is not null                      as has_hh_readings
from ref_meterpoints mp
         inner join ref_meters met on mp.account_id = met.account_id and
                                      mp.meter_point_id = met.meter_point_id and
                                      met.removeddate is null
         left join cte_meters_attr elec_met_type on elec_met_type.account_id = mp.account_id and
                                                    elec_met_type.meter_point_id = mp.meter_point_id and
                                                    elec_met_type.meter_id = met.meter_id and
                                                    elec_met_type.name = 'MeterType'
         left join cte_meters_attr gas_met_type on gas_met_type.account_id = mp.account_id and
                                                   gas_met_type.meter_point_id = mp.meter_point_id and
                                                   gas_met_type.meter_id = met.meter_id and
                                                   gas_met_type.name = 'Meter_Mechanism_Code'
         left join cte_device_id_map dev_ids on mp.meterpointnumber = dev_ids.mpxn
         left join (select distinct deviceid, 'E' as fuel
                    from aws_smart_stage2_extracts.smart_stage2_smarthalfhourlyreads_elec
                    union
                    select distinct deviceid, 'G' as fuel
                    from aws_smart_stage2_extracts.smart_stage2_smarthalfhourlyreads_gas) hh_read_device_ids
                   on upper(hh_read_device_ids.deviceid) = dev_ids.device_id and
                      hh_read_device_ids.fuel = dev_ids.fuel
where getdate() between greatest(mp.associationstartdate, mp.supplystartdate) and nvl(least(mp.associationenddate, mp.supplyenddate), getdate() + 1)
order by account_id, meter_point_id, meter_id
;

select * from temp_meter_info where not is_s2_meter and has_hh_readings