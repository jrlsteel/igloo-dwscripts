/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE
*/
select count(*) from aws_s3_ensec_api_extracts.cdb_stagetariffhistory; --6761

select count(*) from (
select distinct account_id,tariffname,startdate,enddate from aws_s3_ensec_api_extracts.cdb_stagetariffhistory); --6761

-- drop table ref_tariff_history;
create table ref_tariff_history
(
	account_id bigint encode delta,
	tariff_name varchar(255),
	start_date timestamp,
	end_date timestamp,
	discounts varchar(255),
	tariff_type varchar(255),
	exit_fees varchar(255)
)
;

alter table ref_tariff_history owner to igloo;
;

-- drop table ref_tariff_history_audit;
create table ref_tariff_history_audit
(
	account_id bigint encode delta,
	tariff_name varchar(255),
	start_date timestamp,
	end_date timestamp,
	discounts varchar(255),
	tariff_type varchar(255),
	exit_fees varchar(255),
	etlchangetype varchar(1),
	etlchange timestamp
)
;
alter table ref_tariff_history_audit owner to igloo;


-- New or Updated sql for audit table
-- insert into ref_tariff_history_audit (
select cast (s.account_id as bigint) as account_id,
	trim (s.tariffname) as tariffname,
	cast (s.startdate as timestamp) as startdate,
	cast (s.enddate as timestamp) as enddate,
	trim(s.discounts) as discounts,
	trim(s.tarifftype) as tariff_type,
	trim(s.exitfees) as exit_fees,
  case when trim(r.tariff_name) is null then 'n' else 'u' end as etlchangetype,
  current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stagetariffhistory s
left outer join ref_tariff_history r ON
      cast (s.account_id as bigint) = r.account_id and
			trim (s.tariffname) = trim(r.tariff_name) and
			cast (nullif(s.startdate,'') as timestamp) = r.start_date
where
			cast (nullif(s.enddate, '') as timestamp) != r.end_date and
			trim(s.discounts) != trim(r.discounts) or
	 		trim(s.tarifftype) != trim(r.tariff_type) or
   		trim(s.exitfees) != trim(r.exit_fees)
	 		or trim(r.tariff_name) is null
--     )
;



--Insert to overwrite ref table
delete from ref_tariff_history;
-- insert into ref_tariff_history (
select 	cast (s.account_id as bigint) as account_id,
	trim (s.tariffname) as tariffname,
	cast (nullif(trim(s.startdate), '') as timestamp) as startdate,
	cast (nullif(trim(s.enddate), '') as timestamp) as enddate,
	trim(s.discounts) as discounts,
	trim(s.tarifftype) as tariff_type,
	trim(s.exitfees) as exit_fees
from aws_s3_ensec_api_extracts.cdb_stagetariffhistory s
-- )
;

-- TESTING --
--Run1:
--After 1st run
select count(*) from  aws_s3_ensec_api_extracts.cdb_stagetariffhistory s; --6761
select count(*) from ref_tariff_history r; --6761
select r.etlchangetype,r.etlchange, count(*) from ref_tariff_history_audit r group by r.etlchangetype, r.etlchange;

--update
update ref_tariff_history set tariff_type = 'Fixed' where account_id = 3895;
-- 2 rows affected

--delete
delete from ref_tariff_history where account_id = 3742;
-- 2 rows affected

--Run2:
--Run the process again
-- Audit Checks
select r.etlchange, r.etlchangetype, count(1) from ref_tariff_history_audit r group by r.etlchange, r.etlchangetype;
--1 new rows
--1 updated rows

