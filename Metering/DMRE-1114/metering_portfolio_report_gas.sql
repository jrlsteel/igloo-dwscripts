drop table ref_calculated_metering_portfolio_gas_report
create table ref_calculated_metering_portfolio_gas_report as
select state.account_id,
       left(postcode, len(postcode) - 3) as Postcode,
       state.acc_stat,
       mp_gas.supplystartdate,
       mp_gas.associationstartdate,
       state.aed                         as associationenddate,
       state.sed                         as supplyenddate,
       state.home_move_in,
       vmrrs.attribute_value             as data_consent,
       replace(vmrp.psr,',',' ')               as psr,
       'DD'                              as payment_type,
       th.tariff_name,
       mp_gas.meter_point_id,
       mp_gas.meterpointnumber          as mpan,
       mp_gas.meterpointtype,
       smef.deviceid,
       smef.firmware_version,
       smef.manufacturer,
       smef.type,
       smef.device_status,
       smef.commisioned_date,
       accs.billdayofmonth,
       accs.nextbilldate,
       mt_gas.meter_id,
       mt_gas.meterserialnumber,
       mt_gas.installeddate,
       reg_elec.register_id,
       vmri.meterreadingstatusuid,
       vmri.meterreadingtypeuid,
       vmri.meterreadingsourceuid,
       vmri.meterreadingdatetime,
       max(case
             when mpa_gas.attributes_attributename = 'LDZ' then mpa_gas.attributes_attributevalue
               end)                      as mpa_gas_LDZ,
       max(case
             when mpa_gas.attributes_attributename = 'Gas_Meter_Location_Code' then mpa_gas.attributes_attributevalue
               end)                      as mta_gas_meter_loc_code,
       max(case
             when mpa_gas.attributes_attributename = 'MeterMakeAndModel'
                     then replace(replace(mpa_gas.attributes_attributevalue,',',' '),'"',' ')
               end)                      as mpa_gas_meter_make_model,
       max(case
             when mta_gas.metersattributes_attributename = 'Meter_Mechanism_Code' then mta_gas.metersattributes_attributevalue
               end)                      as mta_gas_meter_type,
       max(case
             when trim(mta_gas.metersattributes_attributename) = 'Year_Of_Manufacture'
                     then mta_gas.metersattributes_attributevalue
               end)                      as mta_gas_meter_year_manu,
       max(case
             when mta_gas.metersattributes_attributename = 'Manufacture_Code'
                     then mta_gas.metersattributes_attributevalue
               end)                      as mta_gas_meter_manu_code,
       max(case
             when mta_gas.metersattributes_attributename = 'Model_Code'
                     then mta_gas.metersattributes_attributevalue
               end)                      as mta_gas_meter_model_code,
       getdate()                         as etlchange
from vw_meterpoint_live_state state
       inner join ref_meterpoints mp_gas on state.account_id = mp_gas.account_id and
                                             mp_gas.meterpointtype = 'G'
       left outer join ref_meterpoints_attributes mpa_gas
         on mp_gas.account_id = mpa_gas.account_id and mp_gas.meter_point_id = mpa_gas.meter_point_id and
            attributes_effectivetodate is null
       left outer join ref_meters mt_gas
         on mp_gas.account_id = mt_gas.account_id and mp_gas.meter_point_id = mt_gas.meter_point_id and
            mt_gas.removeddate is null
       left outer join ref_meters_attributes mta_gas
         on mp_gas.account_id = mta_gas.account_id and mt_gas.meter_point_id = mta_gas.meter_point_id and
            mt_gas.meter_id = mta_gas.meter_id
       left outer join ref_registers reg_elec
         on mt_gas.account_id = reg_elec.account_id and mt_gas.meter_id = reg_elec.meter_id
       left outer join aws_s3_stage2_extracts.stage2_accountsettings accs on mp_gas.account_id = accs.account_id
       left outer join vw_metering_report_reads_info vmri
         on mp_gas.account_id = vmri.account_id and reg_elec.register_id = vmri.register_id
       left outer join aws_met_stage1_extracts.met_igloo_smart_metering_estate_firmware smef
         on mp_gas.meterpointnumber = smef.mpxn_number and smef.device_status <> 'InstalledNotCommissioned'
       left outer join vw_supply_contracts_with_occ_accs vscoa on mp_gas.account_id = vscoa.external_id
       left outer join ref_cdb_addresses rca on vscoa.supply_address_id = rca.id
       left outer join vw_metering_report_read_schedule vmrrs on vscoa.external_id = vmrrs.external_id
       left outer join ref_tariff_history th on vscoa.external_id = th.account_id and th.end_date is null
       left outer join vw_metering_report_psr vmrp on vscoa.external_id = vmrp.external_id
--where   vscoa.external_id = 84505
group by state.account_id,
         left(postcode, len(postcode) - 3),
         state.acc_stat,
         state.aed,
         state.sed,
         state.home_move_in, mp_gas.account_id, mp_gas.meter_point_id, mp_gas.meterpointnumber,
         vmrrs.attribute_value,
         replace(vmrp.psr,',',' ') ,
         th.tariff_name,
         mp_gas.meterpointtype,
         smef.deviceid,
         smef.firmware_version,
         smef.manufacturer,
         smef.type,
         smef.device_status,
         smef.commisioned_date,
         mp_gas.supplystartdate,
         mp_gas.associationstartdate,
         mp_gas.issmart,
         mp_gas.issmartcommunicating,
         accs.billdayofmonth,
         accs.nextbilldate,
         mt_gas.meter_id,
         mt_gas.meterserialnumber,mt_gas.installeddate,
         reg_elec.register_id, vmri.meterreadingstatusuid,
         vmri.meterreadingtypeuid,
         vmri.meterreadingsourceuid,
         vmri.meterreadingdatetime
order by mp_gas.account_id, mp_gas.meter_point_id, mp_gas.supplystartdate, mt_gas.meter_id


