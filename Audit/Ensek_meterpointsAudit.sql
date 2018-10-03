/*
PRECHECK QUERIES TO VALIDATE JOIN KEYS

-- Get total count
select count(1) from ref_meterpoints s; --36915

--Get distinct count on unique keys
select count(1) from
(select distinct account_id, meter_point_id from ref_meterpoints); --36913

--To get the duplicated rows
select account_id, meter_point_id from ref_meterpoints
group by account_id, meter_point_id
having count(1) > 1;
--2 duplicate rows

--duplicate meterpoints
-- account_id  meter_point_id
-- 4115	      6414
-- 6291	      10577
*/



create table ref_meterpoints
(
	account_id bigint encode delta,
	meter_point_id bigint encode delta,
	meterpointnumber bigint,
	associationstartdate timestamp,
	associationenddate timestamp,
	supplystartdate timestamp,
	supplyenddate timestamp,
	issmart boolean,
	issmartcommunicating boolean,
	meterpointtype varchar(255)
)
;

alter table ref_meterpoints owner to igloo
;

create table ref_meterpoints_audit
(
	account_id bigint encode delta,
	meter_point_id bigint encode delta,
	meterpointnumber bigint,
	associationstartdate timestamp,
	associationenddate timestamp,
	supplystartdate timestamp,
	supplyenddate timestamp,
	issmart boolean,
	issmartcommunicating boolean,
	meterpointtype varchar(255),
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_meterpoints_audit owner to igloo
;
-- New and update for audit tables
-- insert into ref_meterpoints_audit
select
		s.account_id,
		s.meter_point_id,
		s.meterpointnumber,
		cast (nullif(s.associationstartdate,'') as timestamp) as associationstartdate,
		cast (nullif(s.associationenddate, '') as timestamp) as associationenddate,
		cast (nullif(s.supplystartdate, '') as timestamp) as supplystartdate,
		cast (nullif(s.supplyenddate, '') as timestamp) as supplyenddate,
		s.issmart,
		s.issmartcommunicating,
		s.meterpointtype,
		case when r.meter_point_id is null then 'n' else 'u' end as etlchangetype,
		current_timestamp
from aws_s3_ensec_api_extracts.cdb_meterpoints s
       left outer join ref_meterpoints r
       ON s.account_id = r.account_id
       and s.meter_point_id = r.meter_point_id
where (s.meterpointnumber !=r.meterpointnumber
		or cast (nullif(s.associationstartdate,'') as timestamp) != r.associationstartdate
		or cast (nullif(s.associationenddate, '') as timestamp) != r.associationenddate
		or cast (nullif(s.supplystartdate, '') as timestamp) != r.supplystartdate
		or cast (nullif(s.supplyenddate, '') as timestamp) !=r.supplyenddate
		or s.issmart != r.issmart
		or s.issmartcommunicating != r.issmartcommunicating
		or s.meterpointtype != r.meterpointtype
		or r.meter_point_id is null);

-- Insert to overwrite ref tables
-- delete from ref_meterpoints;
-- insert into ref_meterpoints
		select s.account_id,
						s.meter_point_id,
						s.meterpointnumber,
						cast (nullif(s.associationstartdate, '') as timestamp) as associationstartdate,
						cast (nullif(s.associationenddate, '') as timestamp)   as associationenddate,
						cast (nullif(s.supplystartdate, '') as timestamp)      as supplystartdate,
						cast (nullif(s.supplyenddate, '') as timestamp)        as supplyenddate,
						s.issmart,
						s.issmartcommunicating,
						s.meterpointtype
		 from aws_s3_ensec_api_extracts.cdb_meterpoints s;