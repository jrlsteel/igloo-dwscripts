create view vw_metering_report_type as
SELECT deviceid,
       type,
       mpxn_number,
       device_status,
       commisioned_date,
       firmware_version,
       manufacturer,
       fueltype
FROM aws_met_stage1_extracts.met_igloo_smart_metering_estate_firmware
where type in ('CAD','PPMID','IHD')
  WITH NO SCHEMA BINDING