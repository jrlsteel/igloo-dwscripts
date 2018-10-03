/*
-- PRECHECK QUERIES TO VALIDATE JOIN KEYS are UNIQUE

-- Get total count from ref_meters
select count(1) from ref_meters_attributes s; --58235

--Get distinct count on unique keys
select count(1) from
(select distinct account_id, meter_point_id, meter_id, metersattributes_attributename, metersattributes_attributedescription from ref_meters_attributes
); --58228

--To get the duplicated rows
select account_id,
meter_point_id,
meter_id,
metersattributes_attributename,
metersattributes_attributedescription
from ref_meters_attributes
group by
account_id,
meter_point_id,
meter_id,
metersattributes_attributename,
metersattributes_attributedescription
having count(1) > 1;
--7 duplicate rows

--Duplicate metersattributes
-- account_id  meter_point_id
-- 6291        10066

*/



create table ref_meters_attributes
(
	account_id bigint encode delta,
	meter_point_id bigint encode delta,
	meter_id bigint encode delta32k,
	metersattributes_attributename varchar(255),
	metersattributes_attributedescription varchar(255),
	metersattributes_attributevalue varchar(255)
)
;

alter table ref_meters_attributes owner to igloo
;

create table ref_meters_attributes_audit
(
	account_id bigint encode delta,
	meter_point_id bigint encode delta,
	meter_id bigint encode delta32k,
	metersattributes_attributename varchar(255),
	metersattributes_attributedescription varchar(255),
	metersattributes_attributevalue varchar(255),
	etlchangetype varchar(1),
	etlchange timestamp
)
;

alter table ref_meters_attributes_audit owner to igloo
;


--  Updated MetersAttributes SQL
-- insert into ref_meters_attributes_audit
select
	s.account_id,
	s.meter_point_id,
	s.meter_id,
	s.metersattributes_attributename,
	s.metersattributes_attributedescription,
	s.metersattributes_attributevalue,
	case when (r.metersattributes_attributename is null or trim(r.metersattributes_attributename) = '') then 'n' else 'u' end as etlchangetype,
	current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_metersattributes s
left outer join ref_meters_attributes r
      ON s.account_id = r.account_id
      and s.meter_point_id = r.meter_point_id
      and s.meter_id = r.meter_id
      and s.metersattributes_attributename = r.metersattributes_attributename
      and s.metersattributes_attributedescription = r.metersattributes_attributedescription
where s.metersattributes_attributevalue != r.metersattributes_attributevalue
			or (r.metersattributes_attributename is null or trim(r.metersattributes_attributename) = '');

-- Overwrite for ref table
-- insert into ref_meters_attributes
select
	s.account_id as account_id,
	s.meter_point_id as meter_point_id,
	s.meter_id as meter_id,
	s.metersattributes_attributename as metersattributes_attributename,
	s.metersattributes_attributedescription as metersattributes_attributedescription,
	s.metersattributes_attributevalue as metersattributes_attributevalue
from aws_s3_ensec_api_extracts.cdb_metersattributes s;
