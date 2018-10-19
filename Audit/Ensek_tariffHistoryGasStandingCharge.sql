/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE
*/
select count(*) from aws_s3_ensec_api_extracts.cdb_stagetariffhistorygasstandcharge ; --6012

select count(*) from (
select distinct account_id,tariffname,startdate,enddate,name from aws_s3_ensec_api_extracts.cdb_stagetariffhistorygasstandcharge)
; --6008


select account_id,tariffname,startdate,enddate,name from aws_s3_ensec_api_extracts.cdb_stagetariffhistorygasstandcharge
group by account_id,tariffname,startdate,enddate,name
having count(*)>1
; --6761

-- 4 duplicate rows


drop table ref_tariff_history_gas_sc;
create table ref_tariff_history_gas_sc
(
	account_id bigint encode delta,
	tariff_name varchar(255),
	start_date timestamp,
	end_date timestamp,
	name varchar(255),
	rate double precision,
	registers varchar(255),
	chargeableComponentUID varchar(255)
)
;

alter table ref_tariff_history_gas_sc owner to igloo;
;
drop table ref_tariff_history_gas_sc_audit;
create table ref_tariff_history_gas_sc_audit
(
	account_id bigint encode delta,
	tariff_name varchar(255),
	start_date timestamp,
	end_date timestamp,
	name varchar(255),
	rate double precision,
	registers varchar(255),
	chargeableComponentUID varchar(255),
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_tariff_history_gas_sc_audit owner to igloo;

-- New or Updated sql for audit table
-- insert into ref_tariff_history_gas_sc_audit (

select cast (s.account_id as bigint) as account_id,
	trim (s.tariffname) as tariffname,
	cast (nullif(s.startdate, '') as timestamp) as startdate,
	cast (nullif(s.enddate, '') as timestamp) as enddate,
	trim(s.name) as name,
	cast (s.rate as double precision) as rate,
	trim (s.registers) as registers,
	trim (s.chargeableComponentUID) as chargeableComponentUID,
  case when r.name is null then 'n' else 'u' end as etlchangetype,
  current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stagetariffhistorygasstandcharge s
left outer join ref_tariff_history_gas_sc r ON
      s.account_id = r.account_id and
			trim(s.tariffname) = trim(r.tariff_name) and
			cast (nullif(s.startdate, '') as timestamp) = r.start_date
--
where
	 		cast (nullif(s.enddate, '') as timestamp) != r.end_date or
			trim(s.name) != trim(r.name) or
	 		cast (s.rate as double precision) != trim(r.rate) or
   		trim(s.registers) != trim(r.registers) or
	 		trim (s.chargeableComponentUID) != trim(r.chargeableComponentUID)
	 		or r.name is null
--     )
;

--Insert to overwrite ref table
-- insert into ref_tariff_history_gas_sc (
select 	cast (s.account_id as bigint) as account_id,
	trim (s.tariffname) as tariffname,
	cast (nullif(s.startdate, '') as timestamp) as startdate,
	cast (nullif(s.enddate, '') as timestamp) as enddate,
	trim(s.name) as name,
	cast (s.rate as double precision) as rate,
	trim (s.registers) as registers,
	trim (s.chargeableComponentUID) as chargeableComponentUID
from aws_s3_ensec_api_extracts.cdb_stagetariffhistorygasstandcharge s
-- )
;

-- TESTING --
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagetariffhistorygasstandcharge s; --
select count(*) from ref_tariff_history_gas_sc r; --
select r.etlchangetype,r.etlchange, count(*) from ref_tariff_history_gas_sc_audit r group by r.etlchangetype, r.etlchange;

--update
update ref_tariff_history_gas_sc set rate = 99.99 where account_id = 3895;

--delete
delete from ref_tariff_history_gas_sc where account_id = 3742;

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_tariff_history_gas_sc_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows


