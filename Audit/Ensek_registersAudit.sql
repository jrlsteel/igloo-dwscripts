/*
PRECHECK QUERIES TO VALIDATE JOIN KEYS

-- Get total count
select count(*) from ref_registers s; --36182

--Get distinct count on unique keys
select count(1) from
(select distinct account_id, meter_point_id, meter_id, register_id from ref_registers); --36176


select account_id, meter_point_id, meter_id, register_id from ref_registers
group by account_id, meter_point_id, meter_id, register_id
having count(1) > 1;
--6 duplicate rows

-- --duplicate meterpoints
-- account_id  meter_point_id  meter_id register_id
-- 6291	      10577	          10066	    10361
-- 6291	      10577	          10066	    10360
-- 4115	      6414	          6259	    6454
*/
-- created ref table
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

alter table ref_registers owner to igloo
;
--created audit table
create table ref_registers_audit
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
	registers_tprperioddescription varchar(255),
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_registers_audit owner to igloo
;

-- New or Updated Registers SQL for audit
-- insert into ref_registers_audit
select
		cast (s.account_id as bigint) as account_id,
		cast (s.meter_point_id as bigint) as meter_point_id,
		cast (s.meter_id as bigint) as meter_id,
		cast (s.register_id as bigint) as register_id,
		cast (s.registers_eacaq as double precision) as registers_eacaq,
		trim (s.registers_registerreference) as registers_registerreference,
		trim (s.registers_sourceidtype) as registers_sourceidtype,
		trim (s.registers_tariffcomponent) as registers_tariffcomponent,
		cast (s.registers_tpr as bigint) as registers_tpr,
		trim (s.registers_tprperioddescription) as registers_tprperioddescription,
		case when r.register_id is null then 'n' else 'u' end as etlchangetype,
		current_timestamp etlchange
from aws_s3_ensec_api_extracts.cdb_stageregisters s
       left outer join ref_registers r
       	ON cast (s.account_id as bigint) = r.account_id
       	and cast (s.meter_point_id as bigint) = r.meter_point_id
       	and cast (s.meter_id as bigint) = r.meter_id
       	and cast (s.register_id as bigint) = r.register_id
where
    cast (s.registers_eacaq as double precision) != r.registers_eacaq
		or trim (s.registers_registerreference) != trim(r.registers_registerreference)
		or trim (s.registers_sourceidtype) != trim(r.registers_sourceidtype)
		or trim (s.registers_tariffcomponent) != trim(r.registers_tariffcomponent)
		or cast (s.registers_tpr as bigint) != r.registers_tpr
		or trim (s.registers_tprperioddescription) != trim(r.registers_tprperioddescription)
		or r.register_id is null
;

-- New Registers SQl for ref table
-- delete from ref_registers;
-- insert into ref_registers
select
    cast (s.account_id as bigint) as account_id,
		cast (s.meter_point_id as bigint) as meter_point_id,
		cast (s.meter_id as bigint) as meter_id,
		cast (s.register_id as bigint) as register_id,
		cast (s.registers_eacaq as double precision) as registers_eacaq,
		trim (s.registers_registerreference) as registers_registerreference,
		trim (s.registers_sourceidtype) as registers_sourceidtype,
		trim (s.registers_tariffcomponent) as registers_tariffcomponent,
		cast (s.registers_tpr as bigint) as registers_tpr,
		trim (s.registers_tprperioddescription) as registers_tprperioddescription
from aws_s3_ensec_api_extracts.cdb_stageregisters s;
;

select r.etlchange, r.etlchangetype, count(*) from ref_registers_audit r group by r.etlchange, r.etlchangetype;

select count(*) from aws_s3_ensec_api_extracts.cdb_stageregisters; --38318
select count(*) from ref_registers r; -- 38318


--Testing
--update
update ref_registers set registers_eacaq = 99.10 where account_id = 8427; -- 3 rows

--delete
delete from ref_registers where account_id = 14144; -- 3 rows

