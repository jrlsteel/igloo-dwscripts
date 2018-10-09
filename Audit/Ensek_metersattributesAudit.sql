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
	cast (s.account_id as bigint) as account_id,
	cast (s.meter_point_id as bigint) as meter_point_id,
	cast (s.meter_id as bigint) as meter_id,
	trim (s.metersattributes_attributename) as metersattributes_attributename,
	trim (s.metersattributes_attributedescription) as metersattributes_attributedescription,
	trim (s.metersattributes_attributevalue) as metersattributes_attributevalue,
	case when (r.metersattributes_attributename is null or trim(r.metersattributes_attributename) = '') then 'n' else 'u' end as etlchangetype,
	current_timestamp as etlchange
from aws_s3_ensec_api_extracts.cdb_stagemetersattributes s
left outer join ref_meters_attributes r
      ON cast (s.account_id as bigint) = r.account_id
      and cast (s.meter_point_id as bigint) = r.meter_point_id
      and cast (s.meter_id as bigint) = r.meter_id
      and trim(s.metersattributes_attributename) = trim(r.metersattributes_attributename)
      and trim(s.metersattributes_attributedescription) = trim(r.metersattributes_attributedescription)
where s.metersattributes_attributevalue != r.metersattributes_attributevalue
			or (trim(r.metersattributes_attributename) is null or trim(r.metersattributes_attributename) = '');

-- Overwrite for ref table
-- insert into ref_meters_attributes
select
	cast (s.account_id as bigint) as account_id,
	cast (s.meter_point_id as bigint) as meter_point_id,
	cast (s.meter_id as bigint) as meter_id,
	trim (s.metersattributes_attributename) as metersattributes_attributename,
	trim (s.metersattributes_attributedescription) as metersattributes_attributedescription,
	trim (s.metersattributes_attributevalue) as metersattributes_attributevalue
from aws_s3_ensec_api_extracts.cdb_stagemetersattributes s;

select count(*) from aws_s3_ensec_api_extracts.cdb_stagemetersattributes; --61679
select count(*) from ref_meters_attributes r; -- 61679
select count(*) from aws_s3_ensec_api_extracts.cdb_metersattributes;

select r.etlchangetype, r.etlchange, count(*) from ref_meters_attributes_audit r group by r.etlchangetype, r.etlchange; -- 36702


--TESTING--
-- test new rows
delete from ref_meters where account_id=11869; -- 3 rows
-- test updated rows
update ref_meters set removeddate = current_timestamp where account_id = 6602;-- 3 rows

--Found some erroneous data. Reason: delimiter found in some columns hence the columns are split wrong
select * from aws_s3_ensec_api_extracts.cdb_stagemetersattributes s where s.meter_id like '%C%';

select * from aws_s3_ensec_api_extracts.cdb_metersattributes s where s.meter_id like '%C%';
select account_id from ref_meters s where s.meter_id in(33996,
32782,
33119,
32730,
38058,
37291,
33052,
33883,
31914,
32921,
32747,
38395,
34980,
34210,
37386,
36110,
36179,
33784,
33807,
32786,
33715,
38342,
33630,
36016,
38088,
32935,
32906,
34793
);
