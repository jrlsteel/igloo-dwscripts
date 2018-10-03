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
		s.account_id,
		s.meter_point_id,
		s.meter_id,
		s.register_id,
		s.registers_eacaq,
		s.registers_registerreference,
		s.registers_sourceidtype,
		s.registers_tariffcomponent,
		s.registers_tpr,
		s.registers_tprperioddescription,
		case when r.register_id is null then 'n' else 'u' end as etlchangetype,
		current_timestamp etlchange
from aws_s3_ensec_api_extracts.cdb_registers s
       left outer join ref_registers r
       	ON s.account_id = r.account_id
       	and s.meter_point_id = r.meter_point_id
       	and s.meter_id = r.meter_id
       	and s.register_id = r.register_id
where (s.registers_eacaq != r.registers_eacaq
		or s.registers_registerreference != r.registers_registerreference
		or s.registers_sourceidtype != r.registers_sourceidtype
		or s.registers_tariffcomponent != r.registers_tariffcomponent
		or s.registers_tpr != r.registers_tpr
		or s.registers_tprperioddescription != r.registers_tprperioddescription
		or r.register_id is null);

-- New Registers SQl for ref table
-- delete from ref_registers;
-- insert into ref_registers
select
		s.account_id as account_id,
		s.meter_point_id as meter_point_id,
		s.meter_id as meter_id,
		s.register_id as register_id,
		s.registers_eacaq as registers_eacaq,
		s.registers_registerreference as registers_registerreference,
		s.registers_sourceidtype as registers_sourceidtype,
		s.registers_tariffcomponent as registers_tariffcomponent,
		s.registers_tpr as registers_tpr,
		s.registers_tprperioddescription as registers_tprperioddescription
from aws_s3_ensec_api_extracts.cdb_registers s;
