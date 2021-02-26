create table temp_meets_eli as
select sc_owner.user_id,
       dcf.account_id                                as ensek_account_id,
       nvl(account_summaries.num_elec_with_hh, 0)    as num_smart_comm_elec,
       nvl(account_summaries.num_gas_with_hh, 0)     as num_smart_comm_gas,
       -- attribute value id 132 is "half hourly" under smart consent
       nvl(attr_consent.attribute_value_id, 0) = 132 as hh_consent,
       dcf.account_status,

       -- additional reporting info
       account_summaries.num_elec,
       account_summaries.num_elec_dcc_enabled,
       account_summaries.num_elec_with_hh,
       account_summaries.num_elec_s2,
       account_summaries.num_gas,
       account_summaries.num_gas_dcc_enabled,
       account_summaries.num_gas_with_hh,
       account_summaries.num_gas_s2,

       getdate()                                     as etlchange

from ref_calculated_daily_customer_file dcf
         left join (select account_id,
                           sum((meterpointtype = 'E')::int)                     as num_elec,
                           sum((meterpointtype = 'E' and is_dcc_enabled)::int)  as num_elec_dcc_enabled,
                           sum((meterpointtype = 'E' and has_hh_readings)::int) as num_elec_with_hh,
                           sum((meterpointtype = 'E' and is_S2_meter)::int)     as num_elec_s2,
                           sum((meterpointtype = 'G')::int)                     as num_gas,
                           sum((meterpointtype = 'G' and is_dcc_enabled)::int)  as num_gas_dcc_enabled,
                           sum((meterpointtype = 'G' and has_hh_readings)::int) as num_gas_with_hh,
                           sum((meterpointtype = 'G' and is_S2_meter)::int)     as num_gas_s2
                    from (select mp.account_id,
                                 mp.meter_point_id,
                                 met.meter_id,
                                 mp.meterpointtype,
                                 left(meter_type.metersattributes_attributevalue, 2) = 'S2' as is_S2_meter,
                                 dev_ids.device_id is not null                              as is_dcc_enabled,
                                 dev_ids.device_id,
                                 hh_read_device_ids.deviceid is not null                    as has_hh_readings
                          from ref_meterpoints mp
                                   inner join ref_meters met on mp.account_id = met.account_id and
                                                                mp.meter_point_id = met.meter_point_id and
                                                                met.removeddate is null
                              -- MeterType attribute is for elec meter type, Meter_Mechanism_Code is for gas meter types
                                   left join ref_meters_attributes meter_type
                                             on meter_type.account_id = mp.account_id and
                                                meter_type.meter_point_id = mp.meter_point_id and
                                                meter_type.meter_id = met.meter_id and
                                                ((mp.meterpointtype = 'E' and
                                                  meter_type.metersattributes_attributename = 'MeterType')
                                                    or (mp.meterpointtype = 'G' and
                                                        meter_type.metersattributes_attributename = 'Meter_Mechanism_Code'))
                                   left join vw_smart_device_id_mpxn_map dev_ids on mp.meterpointnumber = dev_ids.mpxn
                              -- join to the device IDs which have provided half hourly readings within the past month
                                   left join (select distinct deviceid, 'E' as fuel
                                              from aws_smart_stage2_extracts.smart_stage2_smarthalfhourlyreads_elec
                                              where "timestamp"::timestamp > dateadd(months, -1, getdate())
                                              union
                                              select distinct deviceid, 'G' as fuel
                                              from aws_smart_stage2_extracts.smart_stage2_smarthalfhourlyreads_gas
                                              where "timestamp"::timestamp > dateadd(months, -1, getdate())) hh_read_device_ids
                                             on upper(hh_read_device_ids.deviceid) = dev_ids.device_id and
                                                hh_read_device_ids.fuel = dev_ids.fuel
-- where the meterpoint has no end date set
                          where least(mp.associationenddate, mp.supplyenddate) is null
                          order by account_id, meter_point_id, meter_id) meter_level_summaries
                    group by account_id) account_summaries on dcf.account_id = account_summaries.account_id
         left join ref_cdb_supply_contracts sc on sc.external_id = dcf.account_id
    -- attribute type 23 is smart consent
         left join ref_cdb_attributes attr_consent
                   on attr_consent.attribute_type_id = 23 and attr_consent.entity_id = sc.id
         left join ref_cdb_user_permissions sc_owner on sc_owner.permissionable_type = 'App\\SupplyContract' and
                                                        sc_owner.permission_level = 0 and
                                                        sc_owner.permissionable_id = sc.id
         left join ref_cdb_users usr on sc_owner.user_id = usr.id
order by user_id, ensek_account_id