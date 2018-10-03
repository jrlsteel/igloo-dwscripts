/*
-- PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE

-- Get total count from ref_meters
select count(1) from ref_meters; --29491

--Get distinct count on unique keys
select count(1) from
(select distinct account_id, meter_point_id, meter_id from ref_meters
); -- 29489

--To get the duplicated rows
select account_id, meter_point_id, meter_id from ref_meters
group by account_id, meter_point_id, meter_id
having count(1) > 1;--2 duplicate rows

--duplicate meterpoints
-- account_id  meter_point_id   meter_id
-- 4115	      6414              6259
-- 6291	      10577             10066
*/

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

alter table ref_meters owner to igloo
;

create table ref_meters_audit
(
	account_id bigint,
	meter_point_id bigint,
	meter_id bigint,
	meterserialnumber varchar(255),
	installeddate timestamp,
	removeddate timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_meters_audit owner to igloo
;



-- Updated Meters SQL
-- insert into ref_meters_audit
select
	s.account_id,
	s.meter_point_id,
	s.meterid,
	s.meterserialnumber,
	cast (nullif(s.installeddate,'') as timestamp) as installeddate,
	cast (nullif(s.removeddate,'') as timestamp) as removeddate,
	case when r.meter_id is null then 'n' else 'u' end as etlchangetype,
	current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_meters s
left outer join ref_meters r
      ON s.account_id = r.account_id
      and s.meter_point_id = r.meter_point_id
      and s.meterid = r.meter_id
where (s.meterserialnumber != r.meterserialnumber
			or cast (nullif(s.installeddate, '') as timestamp) != r.installeddate
			or cast (nullif(s.removeddate, '') as timestamp) != r.removeddate
			or r.meter_id is null);

-- delete from ref_meters;
-- insert into
select
	s.account_id as account_id,
	s.meter_point_id as meter_point_id,
	s.meterid as meter_id,
	s.meterserialnumber as meterserialnumber,
	cast (nullif(s.installeddate,'') as timestamp) installeddate,
	cast (nullif(s.removeddate,'') as timestamp) removeddate
from aws_s3_ensec_api_extracts.cdb_meters s;

