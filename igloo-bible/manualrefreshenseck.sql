truncate ref_account_status;
insert into ref_account_status
(select account_id, status from aws_s3_ensec_api_extracts.cdb_accountstatus);

truncate  ref_registrations_status_gas;
insert into ref_registrations_status_gas
(select account_id, status from aws_s3_ensec_api_extracts.cdb_registrationsgas);

truncate ref_registrations_status_elec;
insert into ref_registrations_status_elec
(select account_id, status from aws_s3_ensec_api_extracts.cdb_registrationselec);

-- Meter Points
truncate ref_meterpoints;
insert into ref_meterpoints
		  (account_id ,
			 meter_point_id,
			 meterpointnumber,
			 associationstartdate,
			 associationenddate,
			 supplystartdate,
			 supplyenddate,
			 issmart,
			 issmartcommunicating,
			 meterpointtype)
select s.account_id ,
			 s.meter_point_id,
			 s.meterpointnumber,
			 nullif(s.associationstartdate,'')::timestamp,
			 nullif(s.associationenddate,'')::timestamp,
			 nullif(s.supplystartdate,'')::timestamp,
			 nullif(s.supplyenddate,'')::timestamp,
			 s.issmart,
			 s.issmartcommunicating,
			 s.meterpointtype
from aws_s3_ensec_api_extracts.cdb_meterpoints s;

--Meterpoints Attributes
truncate ref_meterpoints_attributes;
insert into ref_meterpoints_attributes
				(account_id,
        meter_point_id,
        attributes_attributename,
        attributes_attributedescription,
        attributes_attributevalue,
        attributes_effectivefromdate,
        attributes_effectivetodate)
select  atr.account_id,
        atr.meter_point_id,
        atr.attributes_attributename,
        atr.attributes_attributedescription,
        atr.attributes_attributevalue,
        nullif(atr.attributes_effectivefromdate,''),
        nullif(atr.attributes_effectivetodate,'')
from aws_s3_ensec_api_extracts.cdb_attributes atr;

--Meters
truncate ref_meters;
insert into ref_meters
			(	account_id,
				meter_point_id,
				meter_id,
				meterserialnumber,
				installeddate,
				removeddate)
select 	met.account_id,
				met.meter_point_id,
				met.meterid,
				met.meterserialnumber,
				nullif(met.installeddate,'')::timestamp,
				nullif(met.removeddate,'')::timestamp
from aws_s3_ensec_api_extracts.cdb_meters met;

--Meters Attributes
truncate ref_meters_attributes;
insert into ref_meters_attributes
			(	account_id,
				meter_point_id,
				meter_id,
				metersattributes_attributename,
				metersattributes_attributedescription,
				metersattributes_attributevalue)
select 	met.account_id,
				met.meter_point_id,
				met.meter_id,
				met.metersattributes_attributename,
				met.metersattributes_attributedescription,
				met.metersattributes_attributevalue
from aws_s3_ensec_api_extracts.cdb_metersattributes met;

--Registers
truncate ref_registers;
insert into ref_registers
			(	account_id,
				meter_point_id,
				meter_id,
				register_id,
				registers_eacaq,
				registers_registerreference,
				registers_sourceidtype,
				registers_tariffcomponent,
				registers_tpr,
				registers_tprperioddescription)
select 	reg.account_id,
				reg.meter_point_id,
				reg.meter_id,
				reg.register_id,
				reg.registers_eacaq,
				reg.registers_registerreference,
				reg.registers_sourceidtype,
				reg.registers_tariffcomponent,
				reg.registers_tpr,
				reg.registers_tprperioddescription
from aws_s3_ensec_api_extracts.cdb_registers reg;

--Meters Attributes
truncate ref_registers_attributes;
insert into ref_registers_attributes
			(	account_id,
				meter_point_id,
				meter_id,
        register_id,
				registersattributes_attributename,
				registersattributes_attributedescription,
				registersattributes_attributevalue)
select 	reg.account_id,
				reg.meter_point_id,
				reg.meter_id,
        reg.register_id,
				reg.registersattributes_attributename,
				reg.registersattributes_attributedescription,
				reg.registersattributes_attributevalue
from aws_s3_ensec_api_extracts.cdb_registersattributes reg;


truncate ref_readings_billeable;
insert into ref_readings_billeable
		( account_id,
      meter_point_id,
      reading_registerid,
      reading_id,
      id,
      meterpointid,
      datetime,
      createddate,
      meterreadingsource,
      readingtype,
      reading_value)
select red.account_id,
       red.meter_point_id,
      red.reading_registerid,
      red.reading_id,
      red.id,
      red.meterpointid,
      nullif(red.datetime,'')::timestamp,
      nullif(red.createddate,'')::timestamp,
      red.meterreadingsource,
      red.readingtype,
      red.reading_value
from aws_s3_ensec_api_extracts.cdb_readingsbilleable red;

truncate ref_readings;
insert into ref_readings
		( account_id,
      meter_point_id,
      reading_registerid,
      reading_id,
      id,
      meterpointid,
      datetime,
      createddate,
      meterreadingsource,
      readingtype,
      reading_value)
select red.account_id,
       red.meter_point_id,
      red.reading_registerid,
      red.reading_id,
      red.id,
      red.meterpointid,
      nullif(red.datetime,'')::timestamp,
      nullif(red.createddate,'')::timestamp,
      red.meterreadingsource,
      red.readingtype,
      red.reading_value
from aws_s3_ensec_api_extracts.cdb_readings red;