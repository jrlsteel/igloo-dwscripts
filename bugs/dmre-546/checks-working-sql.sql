select
                                                cast (s.account_id as bigint) as account_id,
                                                cast (s.meter_point_id as bigint) as meter_point_id,
                                                cast (s.meterpointnumber as bigint) as meterpointnumber,
                                                cast (nullif(s.associationstartdate,'') as timestamp) as associationstartdate,
                                                cast (nullif(s.associationenddate, '') as timestamp) as associationenddate,
                                                cast (nullif(s.supplystartdate, '') as timestamp) as supplystartdate,
                                                cast (nullif(s.supplyenddate, '') as timestamp) as supplyenddate,
                                                cast (cast (case when s.issmart = 'True' then 1 else 0 end as int) as boolean) as issmart,
                                                cast (cast (case when s.issmartcommunicating = 'True' then 1 else 0 end as int) as boolean) as issmartcommunicating,
                                                trim(s.meterpointtype),
                                                case when r.meter_point_id is null then 'n' else 'u' end as etlchangetype,
                                                current_timestamp as etlchange
                                        from meterpoints_s3 s
                                               left outer join meterpoints_rs r
                                               ON cast (s.account_id as bigint) = r.account_id
                                               and cast (s.meter_point_id as bigint) = r.meter_point_id
                                        where r.meter_point_id is null or
                                              not (
                                                cast (s.meterpointnumber as bigint ) = r.meterpointnumber
                                                and cast(case when s.associationstartdate='' then '1970-01-01' else s.associationstartdate end as timestamp) =
                                                    coalesce(r.associationstartdate,'1970-01-01')
                                                and cast(case when s.associationenddate='' then '1970-01-01' else s.associationenddate end as timestamp) =
                                                    coalesce(r.associationenddate,'1970-01-01')
                                                and cast(case when s.supplystartdate='' then '1970-01-01' else s.supplystartdate end as timestamp) =
                                                    coalesce(r.supplystartdate,'1970-01-01')
                                                and cast(case when s.supplyenddate='' then '1970-01-01' else s.supplyenddate end as timestamp) =
                                                    coalesce(r.supplyenddate,'1970-01-01')
                                                and cast (cast (case when s.issmart = 'True' then 1 else 0 end as int) as boolean) = r.issmart
                                                and cast (cast (case when s.issmartcommunicating = 'True' then 1 else 0 end as int) as boolean) = r.issmartcommunicating
                                                and trim(s.meterpointtype) = trim(r.meterpointtype)
                                              )

select * from ref_meterpoints_attributes where attributes_attributename = 'MeterMakeAndModel' and attributes_attributevalue like
'%"GEC/ABB ""C11B2%';

delete from ref_meterpoints_attributes where attributes_attributename = 'MeterMakeAndModel' and attributes_attributevalue like
'%"GEC/ABB ""C11B2%';

select * from ref_meterpoints_attributes where account_id =4987
and attributes_attributename = 'MeterMakeAndModel';

select * from ref_meterpoints_attributes
where meter_point_id is null

select * from aws_s3_stage2_extracts.stage2_meterpointsattributes where account_id= 4987
select * from aws_s3_stage2_extracts.stage2_meterpointsattributes where meter_point_id is null

select cast(s.account_id as bigint)                                        as account_id,
       cast(s.meter_point_id as bigint)                                    as meter_point_id,
       trim(s.attributes_attributename)                                    as attributes_attributename,
       trim(s.attributes_attributedescription)                             as attributes_attributedescription,
       trim(s.attributes_attributevalue)                                   as attributes_attributevalue,
       cast(nullif(trim(s.attributes_effectivefromdate), '') as timestamp) as attributes_effectivefromdate,
       cast(nullif(trim(s.attributes_effectivetodate), '') as timestamp)   as attributes_effectivetodate,
       case when r.attributes_attributename is null then 'n' else 'u' end  as etlchangetype,
       current_timestamp                                                   as etlchange
    --from table_s3 s
from aws_s3_stage2_extracts.stage2_meterpointsattributes s --left outer join table_rs r
       left outer join ref_meterpoints_attributes r ON cast(s.account_id as bigint) = r.account_id
                                                         and cast(s.meter_point_id as bigint) = r.meter_point_id
                                                         and
                                                       trim(s.attributes_attributename) = trim(r.attributes_attributename)
                                                         and trim(s.attributes_attributedescription) =
                                                             trim(r.attributes_attributedescription)
where not (cast(coalesce(nullif(trim(s.attributes_effectivefromdate), ''), '1970-01-01') as timestamp) =
           coalesce(r.attributes_effectivefromdate, '1970-01-01')
             or cast(coalesce(nullif(trim(s.attributes_effectivetodate), ''), '1970-01-01') as timestamp) =
                coalesce(r.attributes_effectivetodate, '1970-01-01')
             or trim(s.attributes_attributevalue) = trim(r.attributes_attributevalue))
   or r.attributes_attributename is null