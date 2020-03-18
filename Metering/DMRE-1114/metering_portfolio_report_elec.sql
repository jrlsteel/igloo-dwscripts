drop table ref_calculated_metering_portfolio_elec_report
create table ref_calculated_metering_portfolio_elec_report as
  select state.account_id,
         left(postcode, len(postcode) - 3) as Postcode,
         state.acc_stat,
         vmrc.gain_type,
         vmrc.gain_date,
         vmrc.loss_type,
         vmrc.loss_date,
         mp_elec.supplystartdate,
         mp_elec.associationstartdate,
         state.aed                         as associationenddate,
         state.sed                         as supplyenddate,
         state.home_move_in,
         vmrrs.attribute_value             as data_consent,
         replace(vmrp.psr, ',', ' ')       as psr,
         'DD'                              as payment_type,
         th.tariff_name,
         mp_elec.meter_point_id,
         mp_elec.meterpointnumber          as mpan,
         mp_elec.meterpointtype,
         smef.deviceid,
         smef.firmware_version,
         smef.manufacturer,
         smef.type,
         smef.device_status,
         smef.commisioned_date,
         accs.billdayofmonth,
         accs.nextbilldate,
         mt_elec.meter_id,
         mt_elec.meterserialnumber,
         mt_elec.installeddate,
         reg_elec.register_id,
         reg_elec.registers_tpr,
         vmri.meterreadingstatusuid,
         vmri.meterreadingtypeuid,
         vmri.meterreadingsourceuid,
         vmri.meterreadingdatetime,
         max(case
               when mpa_elec.attributes_attributename = 'Profile Class' then mpa_elec.attributes_attributevalue
                 end)                      as mpa_elec_profile_class,
         max(case
               when mpa_elec.attributes_attributename = 'GSP' then mpa_elec.attributes_attributevalue
                 end)                      as mpa_elec_gsp,
         max(case
               when mpa_elec.attributes_attributename = 'SSC' then mpa_elec.attributes_attributevalue
                 end)                      as mpa_elec_ssc,
         max(case
               when mta_elec.metersattributes_attributename = 'METER_LOCATION'
                       then mta_elec.metersattributes_attributevalue
                 end)                      as mta_elec_meter_loc_code,
         max(case
               when mpa_elec.attributes_attributename = 'MeterMakeAndModel'
                       then replace(replace(mpa_elec.attributes_attributevalue, ',', ' '), '"', ' ')
                 end)                      as mpa_elec_meter_make_model,
         max(case
               when mta_elec.metersattributes_attributename = 'MeterType' then mta_elec.metersattributes_attributevalue
                 end)                      as mta_elec_meter_type,
         getdate()                         as etlchange
  from vw_meterpoint_live_state state
         inner join ref_meterpoints mp_elec on state.account_id = mp_elec.account_id and
                                               mp_elec.meterpointtype = 'E'
         left outer join ref_meterpoints_attributes mpa_elec
           on mp_elec.account_id = mpa_elec.account_id and mp_elec.meter_point_id = mpa_elec.meter_point_id and
              attributes_effectivetodate is null
         left outer join ref_meters mt_elec
           on mp_elec.account_id = mt_elec.account_id and mp_elec.meter_point_id = mt_elec.meter_point_id and
              mt_elec.removeddate is null
         left outer join ref_meters_attributes mta_elec
           on mp_elec.account_id = mta_elec.account_id and mt_elec.meter_point_id = mta_elec.meter_point_id and
              mt_elec.meter_id = mta_elec.meter_id
         left outer join ref_registers reg_elec
           on mt_elec.account_id = reg_elec.account_id and mt_elec.meter_id = reg_elec.meter_id
         left outer join aws_s3_stage2_extracts.stage2_accountsettings accs on mp_elec.account_id = accs.account_id
         left outer join vw_metering_report_reads_info vmri
           on mp_elec.account_id = vmri.account_id and reg_elec.register_id = vmri.register_id
         left outer join aws_met_stage1_extracts.met_igloo_smart_metering_estate_firmware smef
           on mp_elec.meterpointnumber = smef.mpxn_number and smef.device_status <> 'InstalledNotCommissioned'
         left outer join vw_supply_contracts_with_occ_accs vscoa on mp_elec.account_id = vscoa.external_id
         left outer join ref_cdb_addresses rca on vscoa.supply_address_id = rca.id
         left outer join vw_metering_report_read_schedule vmrrs on vscoa.external_id = vmrrs.external_id
         left outer join ref_tariff_history th on vscoa.external_id = th.account_id and th.end_date is null
         left outer join vw_metering_report_psr vmrp on vscoa.external_id = vmrp.external_id
         left outer join vw_metering_report_cot vmrc
           on state.account_id = vmrc.account_id and vmrc.meter_point_id = mp_elec.meter_point_id
      --where   vscoa.external_id = 84505
  group by state.account_id,
           left(postcode, len(postcode) - 3),
           state.acc_stat,
           vmrc.gain_date,
           vmrc.gain_type,
           vmrc.loss_date,
           vmrc.loss_type,
           state.aed,
           state.sed,
           state.home_move_in, mp_elec.account_id, mp_elec.meter_point_id, mp_elec.meterpointnumber,
           vmrrs.attribute_value,
           replace(vmrp.psr, ',', ' '),
           th.tariff_name,
           mp_elec.meterpointtype,
           smef.deviceid,
           smef.firmware_version,
           smef.manufacturer,
           smef.type,
           smef.device_status,
           smef.commisioned_date,
           mp_elec.supplystartdate,
           mp_elec.associationstartdate,
           mp_elec.issmart,
           mp_elec.issmartcommunicating,
           accs.billdayofmonth,
           accs.nextbilldate,
           mt_elec.meter_id,
           mt_elec.meterserialnumber,
           mt_elec.installeddate,
           reg_elec.register_id,
           reg_elec.registers_tpr,
           vmri.meterreadingstatusuid,
           vmri.meterreadingtypeuid,
           vmri.meterreadingsourceuid, vmri.meterreadingdatetime
  order by mp_elec.account_id, mp_elec.meter_point_id, mp_elec.supplystartdate, mt_elec.meter_id


