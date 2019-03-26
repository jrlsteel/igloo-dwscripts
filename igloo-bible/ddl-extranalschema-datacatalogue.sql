create external table spectrum.ensek_meterpoints_meters2(
meterserialnumber varchar(255),
installeddate varchar(255),
removeddate varchar(255),
meterid integer,
meter_point_id integer,
account_id integer)
row format delimited
row delimited by ','
fields terminated by '\t'
stored as textfile
location 's3://igloo-uat-bucket/ensek-meterpoints/Meters/';

create external schema aws_igloo_prod_customerdb
from data catalog
database 'prod-rds-customer'
iam_role 'arn:aws:iam::630944350233:role/AmazonRedshiftRoleCustom'
;


create external schema aws_s3_ensec_api_extracts
from data catalog
database 'ensec-api-extracts'
iam_role 'arn:aws:iam::630944350233:role/AmazonRedshiftRoleCustom'
;

create external schema aws_s3_epc_certificates
from data catalog
database 'epc-certificates'
iam_role 'arn:aws:iam::630944350233:role/AmazonRedshiftRoleCustom'
;

create external schema aws_s3_epc_recommendations
from data catalog
database 'epc-recommendations'
iam_role 'arn:aws:iam::630944350233:role/AmazonRedshiftRoleCustom'
;


SELECT * FROM svv_external_schemas;
select * from svv_external_tables;
select * from aws_s3_ensec_extracts.attributes;

aws_s3_ensec_extracts.readings

--Updated Readings SQL
select s.reading_id,s.account_id,s.meter_point_id
from aws_s3_ensec_extracts.readings s
       inner join ref_readings r
         ON s.reading_id = r.reading_id
where (s.meterreadingsource != r.meterreadingsource
         or s.readingtype != r.readingtype
         or s.reading_value != r.reading_value);

-- Deleted Readings SQL
select s.account_id, s.meter_point_id, s.id, s.datetime,
       s.createddate, s.meterreadingsource, s.reading_id,
       s.reading_registerid, s.readingtype,
       s.reading_value, s.meterpointid
from ref_readings r
       left outer join aws_s3_ensec_extracts.readings s
         ON s.reading_id = r.reading_id
where r.reading_id is null;

-- New Readings SQL
select s.account_id, s.meter_point_id, s.id, s.datetime,
       s.createddate, s.meterreadingsource, s.reading_id,
       s.reading_registerid, s.readingtype,
       s.reading_value, s.meterpointid
from aws_s3_ensec_extracts.readings s
       left outer join ref_readings r
         ON s.reading_id = r.reading_id
where r.reading_id is null;

select count(*)
from aws_s3_ensec_extracts.readings s
       left outer join ref_readings r
         ON s.reading_id = r.reading_id
where r.reading_id is null;

select count(*)
from   ref_meterpoints;





from aws_s3_ensec_extracts.readings s
       inner join ref_meterpoints r
         on s.meter_point_id