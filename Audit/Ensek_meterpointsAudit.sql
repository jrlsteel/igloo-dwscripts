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


drop table ref_meterpoints;
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
	meterpointtype varchar(1)
)
;
alter table ref_meterpoints owner to igloo;


drop table ref_meterpoints_audit;
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
	meterpointtype varchar(1),
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_meterpoints_audit owner to igloo
;

-- New and update for audit tables
-- insert into ref_meterpoints_audit (
select
		cast (s.account_id as bigint) as account_id ,
		cast (s.meter_point_id as bigint) as meter_point_id ,
		cast (s.meterpointnumber as bigint) as meterpointnumber ,
		cast (nullif(s.associationstartdate,'') as timestamp) as associationstartdate,
		cast (nullif(s.associationenddate, '') as timestamp) as associationenddate,
		cast (nullif(s.supplystartdate, '') as timestamp) as supplystartdate,
		cast (nullif(s.supplyenddate, '') as timestamp) as supplyenddate,
		cast (cast (case when s.issmart = 'True' then 1 else 0 end as int) as boolean) as issmart,
		cast (cast (case when s.issmartcommunicating = 'True' then 1 else 0 end as int) as boolean) as issmartcommunicating,
		trim(s.meterpointtype) as meterpointtype,
		case when r.meter_point_id is null then 'n' else 'u' end as etlchangetype,
		current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stagemeterpoints s
       left outer join ref_meterpoints r
       ON cast (s.account_id as bigint) = r.account_id
       and cast (s.meter_point_id as bigint) = r.meter_point_id
where cast (s.meterpointnumber as bigint ) !=r.meterpointnumber
		or cast (nullif(s.associationstartdate,'') as timestamp) != r.associationstartdate
		or cast (nullif(s.associationenddate, '') as timestamp) != r.associationenddate
		or cast (nullif(s.supplystartdate, '') as timestamp) != r.supplystartdate
		or cast (nullif(s.supplyenddate, '') as timestamp) != r.supplyenddate
		or cast (cast (case when s.issmart = 'True' then 1 else 0 end as int) as boolean) != r.issmart
		or cast (cast (case when s.issmartcommunicating = 'True' then 1 else 0 end as int) as boolean) != r.issmartcommunicating
		or trim(s.meterpointtype) != trim(r.meterpointtype)
		or r.meter_point_id is null
-- )
;

-- Insert to overwrite ref tables
-- delete from ref_meterpoints;
-- insert into ref_meterpoints
select cast (s.account_id as bigint) as account_id ,
		cast (s.meter_point_id as bigint) as meter_point_id ,
		cast (s.meterpointnumber as bigint) as meterpointnumber ,
		cast (nullif(s.associationstartdate,'') as timestamp) as associationstartdate,
		cast (nullif(s.associationenddate, '') as timestamp) as associationenddate,
		cast (nullif(s.supplystartdate, '') as timestamp) as supplystartdate,
		cast (nullif(s.supplyenddate, '') as timestamp) as supplyenddate,
		cast (cast (case when s.issmart = 'True' then 1 else 0 end as int) as boolean) as issmart,
		cast (cast (case when s.issmartcommunicating = 'True' then 1 else 0 end as int) as boolean) as issmartcommunicating,
		trim(s.meterpointtype) as meterpointtype
from aws_s3_ensec_api_extracts.cdb_stagemeterpoints s;


select current_timestamp, r.etlchange,r.etlchangetype, count(*) from ref_meterpoints_audit r group by r.etlchange,r.etlchangetype; --39795
select count(*) from ref_meterpoints r; --39795


--TESTING--
-- test new rows
delete from ref_meterpoints where account_id=2340; -- 44 rows
-- test updated rows
update ref_meterpoints set issmart = 'False' where account_id = 2435;-- 45 rows
