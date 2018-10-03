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
-- insert into ref_meterpoints_attributes_audit
select
	s.account_id,
	s.meter_point_id,
	s.attributes_attributename,
	s.attributes_attributedescription,
	s.attributes_attributevalue,
	cast (nullif(s.attributes_effectivefromdate,'') as timestamp) as attributes_effectivefromdate,
	cast (nullif(s.attributes_effectivetodate,'') as timestamp) as attributes_effectivetodate,
	case when r.attributes_attributename is null then 'n' else 'u' end as etlchangetype,
	current_timestamp
from aws_s3_ensec_api_extracts.cdb_meterpointsattributes s
left outer join ref_meterpoints_attributes r
      ON s.account_id = r.account_id
      and s.meter_point_id = r.meter_point_id
      and s.attributes_attributename = r.attributes_attributename
      and s.attributes_attributedescription = r.attributes_attributedescription
where (cast (nullif(s.attributes_effectivefromdate,'') as timestamp) != r.attributes_effectivefromdate
      or cast (nullif(s.attributes_effectivetodate,'') as timestamp) != r.attributes_effectivetodate
      or s.attributes_attributevalue != r.attributes_attributevalue
			or r.attributes_attributename is null);

-- overwrite for ref table
-- delete from ref_meterpoints_attributes;
-- insert into ref_meterpoints_attributes
select s.account_id,
	s.meter_point_id,
	s.attributes_attributename,
	s.attributes_attributedescription,
	s.attributes_attributevalue,
	cast (nullif(s.attributes_effectivefromdate,'') as timestamp) as attributes_effectivefromdate,
	cast (nullif(s.attributes_effectivetodate,'') as timestamp) as attributes_effectivetodate
			from aws_s3_ensec_api_extracts.cdb_meterpointsattributes s;