--drop table ref_readings_internal;
create table if not exists ref_readings_internal
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
	meterreadingdatetime timestamp,
	meterreadingsourceuid varchar(255),
	meterreadingstatusuid varchar(255),
	meterreadingtypeuid varchar(255),
	meterserialnumber varchar(255),
	readingvalue double precision,
	registerreference varchar(255),
	required boolean
);

alter table ref_readings_internal owner to igloo;

-- drop table ref_readings_internal_audit;
create table if not exists ref_readings_internal_audit
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
	meterreadingdatetime timestamp,
	meterreadingsourceuid varchar(255),
	meterreadingstatusuid varchar(255),
	meterreadingtypeuid varchar(255),
	meterserialnumber varchar(255),
	readingvalue double precision,
	registerreference varchar(255),
	required boolean,
	etlchangetype varchar(1),
	etlchange timestamp
);

alter table ref_readings_internal_audit owner to igloo;

-- New or Updated data for audit
insert into ref_readings_internal_audit (
select
  s.accountid,
	s.meterpointid,
	s.meterid,
	s.meterreadingid,
	s.registerid,
	s.registerreadingid,
	s.billable,
	s.haslivecharge,
	s.hasregisteradvance,
	s.meterpointnumber,
	s.meterpointtype,
	cast (s.meterreadingcreateddate as timestamp) meterreadingcreateddate,
	cast (s.meterreadingdatetime as timestamp) meterreadingdatetime,
	s.meterreadingsourceuid,
	s.meterreadingstatusuid,
	s.meterreadingtypeuid,
	s.meterserialnumber,
	s.readingvalue,
	s.registerreference,
	s.required,
	case when r.register_reading_id  is null then 'n' else 'u' end etlchange,
	current_timestamp
from aws_s3_ensec_api_extracts.cdb_readingsinternal s
      left outer join ref_readings_internal r
      		on r.account_id = s.accountid
      		and r.meter_point_id = s.meterpointid
      		and r.meter_id = s.meterid
      		and r.meter_reading_id = s.meterreadingid
					and r.register_id = s.registerid
					and r.register_reading_id = s.registerreadingid
where s.billable != r.billable
			or s.haslivecharge != r.haslivecharge
			or s.hasregisteradvance != r.hasregisteradvance
			or s.meterpointnumber != r.meterpointnumber
			or s.meterpointtype != r.meterpointtype
			or cast (s.meterreadingcreateddate as timestamp) != r.meterreadingcreateddate
			or cast (s.meterreadingdatetime as timestamp) != r.meterreadingcreateddate
			or s.meterreadingsourceuid != r.meterreadingsourceuid
			or s.meterreadingstatusuid != r.meterreadingstatusuid
			or s.meterserialnumber != r.meterserialnumber
			or s.readingvalue != r.readingvalue
			or s.registerreference != r.registerreference
			or r.register_reading_id is null
);

-- Overwrite ref table
insert into ref_readings_internal (
select accountid,
	meterpointid,
	meterid,
	meterreadingid,
	registerid,
	registerreadingid,
	billable,
	haslivecharge,
	hasregisteradvance,
	meterpointnumber,
	meterpointtype,
	cast (meterreadingcreateddate as timestamp) readingcreateddate,
	cast (meterreadingdatetime as timestamp) readingdatetime,
	meterreadingsourceuid,
	meterreadingstatusuid,
	meterreadingtypeuid,
	meterserialnumber,
	readingvalue,
	registerreference,
	required
	from aws_s3_ensec_api_extracts.cdb_readingsinternal
)




