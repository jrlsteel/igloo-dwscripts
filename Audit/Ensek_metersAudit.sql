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
-- insert into ref_meters_audit (
select cast(s.account_id as bigint) as account_id,
       cast(s.meter_point_id as bigint) as meter_point_id,
       cast(s.meterid as bigint) as meterid,
       trim(s.meterserialnumber) as meterserialnumber,
       cast(nullif(s.installeddate, '') as timestamp) as installeddate,
       cast(nullif(s.removeddate, '') as timestamp) as removeddate,
       case when r.meter_id is null then 'n' else 'u' end as etlchangetype,
       current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stagemeters s
       left outer join ref_meters r ON
        cast(s.account_id as bigint) = r.account_id
        and cast(s.meter_point_id as bigint) = r.meter_point_id
        and cast(s.meterid as bigint) = r.meter_id
where trim(s.meterserialnumber) != trim(r.meterserialnumber)
   or cast(nullif(s.installeddate, '') as timestamp) != r.installeddate
   or cast(nullif(s.removeddate, '') as timestamp) != r.removeddate
   or r.meter_id is null
--      )
;

-- overwrite for ref table
-- delete from ref_meters;
-- insert into ref_meters
select cast(s.account_id as bigint) as account_id,
       cast(s.meter_point_id as bigint) as meter_point_id,
       cast(s.meterid as bigint) as meterid,
       trim(s.meterserialnumber) as meterserialnumber,
       cast(nullif(s.installeddate, '') as timestamp) as installeddate,
       cast(nullif(s.removeddate, '') as timestamp) as removeddate
from aws_s3_ensec_api_extracts.cdb_stagemeters s;


select count(*) from aws_s3_ensec_api_extracts.cdb_stagemeters; --36578
select count(*) from ref_meters r; -- 0

select r.etlchangetype, r.etlchange, count(*) from ref_meters_audit r group by r.etlchangetype, r.etlchange; -- 36702


--TESTING--
-- test new rows
delete from ref_meters where account_id=11869; -- 3 rows
-- test updated rows
update ref_meters set removeddate = current_timestamp where account_id = 6602;-- 3 rows

select * from aws_s3_ensec_api_extracts.cdb_stagemeters s where s.account_id = 6602;
select * from ref_meters s where s.account_id = 6602;
