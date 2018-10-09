/*
--PRECHECK QUERIES TO VALIDATE JOIN KEYS
-- check on unique key to join
select count(1) from ref_meterpoints_attributes s; --790621
select count(1) from
(select distinct account_id, meter_point_id,attributes_attributename, attributes_attributedescription
from ref_meterpoints_attributes); --789701

select account_id, meter_point_id,attributes_attributename, attributes_attributedescription
from ref_meterpoints_attributes
group by account_id, meter_point_id,attributes_attributename, attributes_attributedescription
having count(1) > 1;
--920 duplicate rows
*/

-- drop table ref_meterpoints_attributes;
create table ref_meterpoints_attributes
(
	account_id bigint,
	meter_point_id bigint,
	attributes_attributename varchar(255),
	attributes_attributedescription varchar(255),
	attributes_attributevalue varchar(255),
	attributes_effectivefromdate timestamp,
	attributes_effectivetodate timestamp
)
;

alter table ref_meterpoints_attributes owner to igloo;

create table ref_meterpoints_attributes_audit
(
	account_id bigint,
	meter_point_id bigint,
	attributes_attributename varchar(255),
	attributes_attributedescription varchar(255),
	attributes_attributevalue varchar(255),
	attributes_effectivefromdate timestamp,
	attributes_effectivetodate timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_meterpoints_attributes_audit owner to igloo
;

--new or update for audit table

-- Updated MeterPoints_Attributes SQL
-- Insert into ref_meterpoints_attributes_audit

select
    cast (s.account_id as bigint) as account_id,
    cast (s.meter_point_id as bigint) as meter_point_id,
    trim (s.attributes_attributename) as attributes_attributename,
    trim (s.attributes_attributedescription) as attributes_attributedescription,
    trim (s.attributes_attributevalue) as attributes_attributevalue,
    cast (nullif (s.attributes_effectivefromdate, '') as timestamp) as attributes_effectivefromdate,
    cast (nullif (s.attributes_effectivetodate, '') as timestamp) as attributes_effectivetodate,
    case when r.attributes_attributename is null then 'n' else 'u' end as etlchangetype,
    current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stagemeterpointsattributes s
left outer join ref_meterpoints_attributes r
      ON cast (s.account_id as bigint) = r.account_id
      and cast (s.meter_point_id as bigint) = r.meter_point_id
      and trim (s.attributes_attributename) = trim (r.attributes_attributename)
      and trim (s.attributes_attributedescription) = trim (r.attributes_attributedescription)
where cast (nullif (s.attributes_effectivefromdate, '') as timestamp) != r.attributes_effectivefromdate
  or cast (nullif (s.attributes_effectivetodate, '') as timestamp) != r.attributes_effectivetodate
  or trim (s.attributes_attributevalue) != trim (r.attributes_attributevalue)
  or trim(r.attributes_attributename) = ''
--     )
;

-- overwrite for ref table
-- delete from ref_meterpoints_attributes;
-- insert into ref_meterpoints_attributes
select cast (s.account_id as bigint) as account_id,
    cast (s.meter_point_id as bigint) as meter_point_id,
    trim (s.attributes_attributename) as attributes_attributename,
    trim (s.attributes_attributedescription) as attributes_attributedescription,
    trim (s.attributes_attributevalue) as attributes_attributevalue,
    cast (nullif (s.attributes_effectivefromdate, '') as timestamp) as attributes_effectivefromdate,
    cast (nullif (s.attributes_effectivetodate, '') as timestamp) as attributes_effectivetodate
from aws_s3_ensec_api_extracts.cdb_stagemeterpointsattributes s;

select count(*) from aws_s3_ensec_api_extracts.cdb_stagemeterpointsattributes; --882683
select r.attributes_attributename from aws_s3_ensec_api_extracts.cdb_stagemeterpointsattributes r
where trim(r.attributes_attributename) is null
group by r.attributes_attributename
; --882683
select count(*) from ref_meterpoints_attributes r; -- 882683

select r.etlchangetype, r.etlchange, count(*) from ref_meterpoints_attributes_audit r group by r.etlchangetype, r.etlchange; -- 880675


--TESTING--
-- test new rows
delete from ref_meterpoints_attributes where account_id=14024; --
-- test updated rows
update ref_meterpoints_attributes set attributes_effectivefromdate = current_timestamp where account_id = 5928;-- 45 rows

