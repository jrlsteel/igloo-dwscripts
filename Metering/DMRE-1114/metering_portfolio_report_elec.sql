select state.account_id,
       left(postcode, len(postcode) - 3) as  Postcode,
       state.acc_stat,
       mp_elec.supplystartdate,
       mp_elec.associationstartdate,
       state.aed                as associationenddate,
       state.sed                as supplyenddate,
       state.home_move_in,
       mp_elec.meter_point_id,
       mp_elec.meterpointnumber as mpan,
       mp_elec.meterpointtype,
       smef."device id",
       smef."firmware version",
       smef.manufacturer,
       smef.type,
       smef."device status",
       smef."commisioned date",
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
               end)             as mpa_elec_profile_class,
       max(case
             when mpa_elec.attributes_attributename = 'GSP' then mpa_elec.attributes_attributevalue
               end)             as mpa_elec_gsp,
       max(case
             when mpa_elec.attributes_attributename = 'SSC' then mpa_elec.attributes_attributevalue
               end)             as mpa_elec_ssc,
       max(case
             when mta_elec.metersattributes_attributename = 'METER_LOCATION'
                     then mta_elec.metersattributes_attributevalue
               end)             as mta_elec_meter_loc_code,
       max(case
             when mpa_elec.attributes_attributename = 'MeterMakeAndModel' then '"'+ mpa_elec.attributes_attributevalue +'"'
               end)             as mpa_elec_meter_make_model,
       max(case
             when mta_elec.metersattributes_attributename = 'MeterType' then mta_elec.metersattributes_attributevalue
               end)             as mta_elec_meter_type,
       max(case
             when mta_elec.metersattributes_attributename = 'Year_Of_Manufacture'
                     then mta_elec.metersattributes_attributevalue
               end)             as mta_elec_meter_year_manu,
       max(case
             when mta_elec.metersattributes_attributename = 'Manufacture_Code'
                     then mta_elec.metersattributes_attributevalue
               end)             as mta_elec_meter_manu_code,
       max(case
             when mta_elec.metersattributes_attributename = 'Model_Code'
                     then mta_elec.metersattributes_attributevalue
               end)             as mta_elec_meter_model_code
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
         on mp_elec.meterpointnumber = smef."mpxn number" and smef."device status" <> 'InstalledNotCommissioned'
       left outer join vw_supply_contracts_with_occ_accs vscoa on mp_elec.account_id =vscoa.external_id
       left outer join ref_cdb_addresses rca on vscoa.supply_address_id  = rca.id
group by state.account_id,
        left(postcode, len(postcode) - 3),
         state.acc_stat,
         state.aed,
         state.sed,
         state.home_move_in, mp_elec.account_id, mp_elec.meter_point_id, mp_elec.meterpointnumber,
         mp_elec.meterpointtype,
         smef."device id",
         smef."firmware version",
         smef.manufacturer,
         smef.type,
          smef."device status",
         smef."commisioned date",
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
         vmri.meterreadingsourceuid,
         vmri.meterreadingdatetime
order by mp_elec.account_id, mp_elec.meter_point_id, mp_elec.supplystartdate, mt_elec.meter_id


SELECT left(postcode, len(postcode) - 3) as postcode FROM ref_cdb_addresses group by left(postcode, len(postcode) - 3)

create or replace view vw_metering_report_reads_info as
select max(
         case when y.n = 1 then estimation_value else 0 end) over (partition by y.register_id) latest_eac,
       max(
         case when y.n = 2 then estimation_value else 0 end) over (partition by y.register_id) previous_eac,
       y.account_id,
       y.meterpointnumber,
       y.registerreference,
       y.register_id,
       y.no_of_digits,
       y.meterreadingdatetime,
       y.meterreadingcreateddate,
       y.meterreadingsourceuid,
       y.meterreadingtypeuid,
       y.meterreadingstatusuid,
       y.corrected_reading,
       y.total_reads
from (select r.*,
             dense_rank() over (partition by account_id, register_id order by meterreadingdatetime desc) n,
             count(*) over (partition by account_id, register_id)                                        total_reads
      from ref_readings_internal_valid r) y
       left outer join ref_estimates_elec_internal ee
         on ee.account_id = y.account_id and y.meterpointnumber = ee.mpan and
            y.registerreference = ee.register_id
              and y.meterserialnumber = ee.serial_number and
            ee.effective_from = y.meterreadingdatetime
where y.n <= 1


  select  '"'+ mpa_elec.attributes_attributevalue ='"'

GRANT USAGE ON SCHEMA aws_met_stage1_extracts TO GROUP read_only_users;
GRANT SELECT ON ALL TABLES IN SCHEMA  aws_met_stage1_extracts TO GROUP read_only_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA  aws_met_stage1_extracts GRANT SELECT ON TABLES TO GROUP read_only_users;