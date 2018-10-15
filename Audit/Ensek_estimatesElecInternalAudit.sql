-- CHECKS TO Get Unique Keys --
select count(*) from ref_estimates_elec_internal; --49619
select count(1) from (
				select distinct s.account_id, s.register_id, s.mpan, s.effective_from, s.effective_to, s.serial_number, s.islive from ref_estimates_elec_internal s
); --49599

-- Unique Keys
-- account_id, register_id, mpan, effective_from, effective_to, serial_number, islive

select s.account_id, s.register_id, s.mpan, s.effective_from, s.effective_to, s.serial_number, s.islive, count(1) from ref_estimates_elec_internal s
                     group by s.account_id, s.register_id, s.mpan, s.effective_from, s.effective_to, s.serial_number, s.islive
having count(1)>1;
--20 duplicate rows

-- Create ref table
create table ref_estimates_elec_internal
(
	account_id bigint encode delta,
	mpan bigint,
	register_id varchar(10) encode bytedict,
	serial_number varchar(20),
	islive boolean,
	effective_from timestamp encode bytedict,
	effective_to timestamp,
	estimation_value double precision
)
;

alter table ref_estimates_elec_internal owner to igloo
;

-- Create audit table
create table ref_estimates_elec_internal_audit
(
	account_id bigint encode delta,
	mpan bigint,
	register_id varchar(10) encode bytedict,
	serial_number varchar(20),
	islive boolean,
	effective_from timestamp encode bytedict,
	effective_to timestamp,
	estimation_value double precision,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_estimates_elec_internal_audit owner to igloo
;


select count(*) from ref_estimates_elec_internal r; --
select count(*) from  aws_s3_ensec_api_extracts.cdb_stageestimateselecinternal s; --49619

-- New or Updated sql for audit table
-- insert into ref_estimates_elec_internal_audit (
select 	cast (s.account_id as bigint) as account_id,
	cast (s.mpan as bigint) as mpan,
	trim (s.registerid) as register_id,
	trim (s.serialnumber) as serialnumber,
	cast (cast (case when s.islive='True' then 1 else 0 end as int) as boolean) as islive,
	cast (nullif(trim(s.effectivefrom), '') as timestamp) as effectivefrom,
	cast (nullif(trim(substring(s.effectiveto,1,19)), '') as timestamp) as effectiveto,
	cast (s.estimationvalue as double precision) as estimation_value,
  case when r.serial_number is null then 'n' else 'u' end as etlchangetype,
  current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stageestimateselecinternal s
left outer join ref_estimates_elec_internal r ON
			cast (s.account_id as bigint) = r.account_id and
      cast (s.mpan as bigint) = r.mpan and
			trim (s.registerid) = trim(r.register_id) and
			trim (s.serialnumber) = trim(r.serial_number) and
			cast (cast (case when s.islive='True' then 1 else 0 end as int) as boolean) = r.islive and
			cast (nullif(s.effectivefrom, '') as timestamp) = r.effective_from and
			cast (nullif(trim(substring(s.effectiveto,1,19)), '') as timestamp) = r.effective_to
where cast (s.estimationvalue as double precision) != r.estimation_value
			or r.serial_number is null
--     )
;
-- New for re table
-- insert into ref_estimates_elec_internal
select 	cast (s.account_id as bigint) as account_id,
	cast (s.mpan as bigint) as mpan,
	trim (s.registerid) as register_id,
	trim (s.serialnumber) as serialnumber,
	cast (cast (case when s.islive='True' then 1 else 0 end as int) as boolean) as islive,
	cast (nullif(trim(s.effectivefrom), '') as timestamp) as effectivefrom,
	cast (nullif(trim(substring(s.effectiveto,1,19)), '') as timestamp) as effectiveto,
	cast (s.estimationvalue as double precision) as estimation_value
from aws_s3_ensec_api_extracts.cdb_stageestimateselecinternal s;

--TESTING
-- After process run
select count(*) from aws_s3_ensec_api_extracts.cdb_stageestimateselecinternal; --49619
select count(*) from ref_estimates_elec_internal; --49619
select count(*) from ref_estimates_elec_internal_audit; --49619
select r.etlchange, r.etlchangetype, count(1) from ref_estimates_elec_internal_audit r group by r.etlchange, r.etlchangetype;
--49619 new rows

-- Update
Update ref_estimates_elec_internal set estimation_value = 9999.99 where account_id = 1937; -- 3 rows

--Delete
Delete from ref_estimates_elec_internal where account_id = 19376; -- 1 rows

--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_estimates_elec_internal_audit r group by r.etlchange, r.etlchangetype;
-- 3 updated rows
-- 1 new rows