create table ref_meterpoints
(
	account_id bigint,
	meter_point_id bigint,
	meterpointnumber bigint,
	associationstartdate timestamp,
	associationenddate timestamp,
	supplystartdate timestamp,
	supplyenddate timestamp,
	issmart boolean,
	issmartcommunicating boolean,
	meterpointtype varchar(1)
)

;



create table ref_meterpoints_attributes
(
	account_id bigint ,
	meter_point_id bigint,
	attributes_attributename varchar(255),
	attributes_attributedescription varchar(255),
	attributes_attributevalue varchar(255),
	attributes_effectivefromdate timestamp,
	attributes_effectivetodate timestamp
)

;


create table ref_meters
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	meterserialnumber varchar(255),
	installeddate timestamp,
	removeddate timestamp
)

;



create table ref_meters_attributes
(
	account_id bigint ,
	meter_point_id bigint,
	meter_id bigint,
	metersattributes_attributename varchar(255),
	metersattributes_attributedescription varchar(255),
	metersattributes_attributevalue varchar(255)
)

;


create table ref_registers
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	register_id bigint,
	registers_eacaq double precision,
	registers_registerreference varchar(255),
	registers_sourceidtype varchar(255),
	registers_tariffcomponent varchar(255),
	registers_tpr bigint,
	registers_tprperioddescription varchar(255)
)

;



create table ref_registers_attributes
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	register_id bigint,
	registersattributes_attributename varchar(255),
	registersattributes_attributedescription varchar(255),
	registersattributes_attributevalue varchar(255)
)

;

create table ref_readings_internal_nrl
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	meter_reading_id bigint,
	register_id bigint,
	register_reading_id bigint,
	billable boolean,
	haslivecharge boolean,
	hasregisteradvance boolean,
	meterpointnumber bigint,
	meterpointtype varchar(1),
	meterreadingcreateddate timestamp,
	meterreadingdatetime timestamp ,
	meterreadingsourceuid varchar(255),
	meterreadingstatusuid varchar(255),
	meterreadingtypeuid varchar(255),
	meterserialnumber varchar(255),
	readingvalue double precision,
	registerreference varchar(255),
	required boolean,
	etlchange timestamp
)

;

create table ref_estimates_elec_internal
(
	account_id bigint,
	mpan bigint,
	register_id varchar(10),
	serial_number varchar(20),
	islive boolean,
	effective_from timestamp,
	effective_to timestamp,
	estimation_value double precision
)

;



create table ref_estimates_gas_internal
(
	account_id bigint,
	mprn bigint,
	register_id varchar(10),
	serial_number varchar(20),
	islive boolean,
	effective_from timestamp,
	effective_to timestamp,
	estimation_value double precision
)

;



create table ref_alp_igloo_cv
(
	ldz text,
	name varchar(500),
	applicable_at date,
	applicable_for date,
	value double precision,
	etlchange timestamp
)

;



create table ref_alp_igloo_daf_wcf
(
	ldz varchar(10),
	regionprofile varchar(500),
	date date,
	forecastdocumentation double precision,
	variance double precision,
	name varchar(500),
	applicable_at date,
	applicable_for date,
	value double precision,
	etlchange timestamp
)
;
