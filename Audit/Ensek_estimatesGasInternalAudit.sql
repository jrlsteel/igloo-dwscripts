create table ref_estimates_gas_internal
(
	account_id bigint encode delta,
	mprn bigint,
	register_id varchar(10),
	serial_number varchar(20),
	islive boolean,
	effective_from timestamp,
	effective_to timestamp,
	estimation_value double precision
)
;

alter table ref_estimates_gas_internal owner to igloo
;

drop table ref_estimates_gas_internal_audit;
create table ref_estimates_gas_internal_audit
(
	account_id bigint encode delta,
	mprn bigint,
	register_id varchar(10),
	serial_number varchar(20),
	islive boolean,
	effective_from timestamp,
	effective_to timestamp,
	estimation_value double precision,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_estimates_gas_internal_audit owner to igloo
;



select count(*) from ref_estimates_gas_internal r; --
select count(*) from  aws_s3_ensec_api_extracts.cdb_stageestimatesgasinternal s; --51316

-- New or Updated sql for audit table
-- insert into ref_estimates_gas_internal_audit (
select 	cast (s.account_id as bigint) as account_id,
	cast (s.mprn as bigint) as mpan,
	trim (s.registerid) as register_id,
	trim (s.serialnumber) as serialnumber,
	cast (cast (case when s.islive='True' then 1 else 0 end as int) as boolean) as islive,
	cast (nullif(trim(s.effectivefrom), '') as timestamp) as effectivefrom,
	cast (nullif(trim(substring(s.effectiveto,1,19)), '') as timestamp) as effectiveto,
	cast (s.estimationvalue as double precision) as estimation_value,
  case when r.serial_number is null then 'n' else 'u' end as etlchangetype,
  current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stageestimatesgasinternal s
left outer join ref_estimates_gas_internal r ON
			cast (s.account_id as bigint) = r.account_id and
      cast (s.mprn as bigint) = r.mprn and
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
-- insert into ref_estimates_gas_internal
select 	cast (s.account_id as bigint) as account_id,
	cast (s.mprn as bigint) as mpan,
	trim (s.registerid) as register_id,
	trim (s.serialnumber) as serialnumber,
	cast (cast (case when s.islive='True' then 1 else 0 end as int) as boolean) as islive,
	cast (nullif(trim(s.effectivefrom), '') as timestamp) as effectivefrom,
	cast (nullif(trim(substring(s.effectiveto,1,19)), '') as timestamp) as effectiveto,
	cast (s.estimationvalue as double precision) as estimation_value
from aws_s3_ensec_api_extracts.cdb_stageestimatesgasinternal s;

--TESTING
-- After process 1st run
select count(*) from aws_s3_ensec_api_extracts.cdb_stageestimatesgasinternal; --51316
select count(*) from ref_estimates_gas_internal; --51316
select count(*) from ref_estimates_gas_internal_audit; --51316
select r.etlchange, r.etlchangetype, count(1) from ref_estimates_gas_internal_audit r group by r.etlchange, r.etlchangetype;
--51316 new rows


-- Update
Update ref_estimates_gas_internal set estimation_value = 9999.99 where account_id = 2128; -- 7 rows

--Delete
Delete from ref_estimates_gas_internal where account_id = 19376; -- 2 rows

--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_estimates_gas_internal_audit r group by r.etlchange, r.etlchangetype;
--2 new rows
--7 updated rows