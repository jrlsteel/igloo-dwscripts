 create table if not exists ref_readings_internal_nosi
 (
 	account_id bigint encode delta32k distkey,
 	meter_point_id bigint encode delta32k,
 	meter_id bigint encode delta32k,
 	meter_reading_id bigint,
 	register_id bigint encode delta32k,
 	register_reading_id bigint,
 	billable boolean,
 	haslivecharge boolean,
 	hasregisteradvance boolean,
 	meterpointnumber bigint,
 	meterpointtype varchar(1) encode bytedict,
 	meterreadingcreateddate timestamp,
 	meterreadingdatetime timestamp encode bytedict,
 	meterreadingsourceuid varchar(255),
 	meterreadingstatusuid varchar(255),
 	meterreadingtypeuid varchar(255),
 	meterserialnumber varchar(255),
 	readingvalue double precision,
 	registerreference varchar(255),
 	required boolean
 )
 diststyle key
 sortkey(account_id, meter_point_id, meter_id, meterreadingcreateddate)
 ;

 alter table ref_readings_internal_nosi owner to igloo
 ;

