SELECT mp.account_id,
       mp.meter_point_id,
       mp.meterpointnumber,
       mp.associationstartdate,
       mp.associationenddate,
       mp.supplystartdate,
       mp.supplyenddate,
       mp.issmart,
       mp.issmartcommunicating,
       mp.meterpointtype
FROM ref_meterpoints mp
     inner join aws_s3_stage1_extracts.stage1_readingsnosigas nosi on mp.meterpointnumber = nosi.meterpointreference and mp.meterpointtype ='G';


SELECT mp.account_id,
       mp.meter_point_id,
       mp.meterpointnumber,
       mp.associationstartdate,
       mp.associationenddate,
       mp.supplystartdate,
       mp.supplyenddate,
       mp.issmart,
       mp.issmartcommunicating,
       mp.meterpointtype
FROM ref_meterpoints mp
     inner join aws_s3_stage1_extracts.stage1_readingsnosigas nosi on mp.meterpointnumber = nosi.meterpointreference and mp.meterpointtype ='G' and
         dateadd(day,1,cast(to_date(nosi.confirmationenddate, 'DD/MM/YYYY') as timestamp)) = mp.supplystartdate
where nosi.meterpointreference = 1494891702
group by mp.account_id,
       mp.meter_point_id,
       mp.meterpointnumber,
       mp.associationstartdate,
       mp.associationenddate,
       mp.supplystartdate,
       mp.supplyenddate,
       mp.issmart,
       mp.issmartcommunicating,
       mp.meterpointtype
having count (*) > 1;


select * from aws_s3_stage1_extracts.stage1_readingsnosigas nosi
where meterpointreference = 1494891702;

select * from ref_meterpoints
where meterpointnumber = 1494891702;

select nosi.*,cast(nosi.confirmationenddate as timestamp), dateadd(day,1,cast(to_date(nosi.confirmationenddate, 'DD/MM/YYYY') as timestamp)), to_date(nosi.confirmationenddate, 'DD/MM/YYYY')
    from aws_s3_stage1_extracts.stage1_readingsnosigas nosi
where meterpointreference = 1494891702;

select count(*) from ref_meterpoints mp
     inner join aws_s3_stage1_extracts.stage1_readingsnosigas nosi on mp.meterpointnumber = nosi.meterpointreference
                                                                           and mp.meterpointtype ='G' and mp.supplystartdate = dateadd(day,1,cast(nosi.confirmationenddate  ;

select count(*) from aws_s3_stage1_extracts.stage1_readingsnosigas nosi;
select count(*) from ref_meterpoints mp;


create table  temp_readings_internal_nosi as
SELECT cast(mp.account_id as bigint) as account_id,
       cast(mp.meter_point_id as bigint) as meter_point_id,
       cast(mt.meter_id as bigint) as meter_id,
       cast(-1 as bigint) as meter_reading_id,
       cast(reg.register_id as bigint) as register_id,
       cast(-1 as bigint) as register_reading_id,
       cast(0 as boolean) as billable,
       cast(0 as boolean) as haslivecharge,
       cast(0 as boolean)as hasregisteradvance,
       cast(nosi.meterpointreference as bigint) as meterpointnumber,
       trim(mp.meterpointtype) as meterpointtype,
       cast(to_date(nosi.lastactualmeterreadingdate, 'DD/MM/YYYY') as timestamp) as meterreadingcreateddate,
       cast(to_date(nosi.lastactualmeterreadingdate, 'DD/MM/YYYY') as timestamp) as metereadingdate,
       'NOSI' as meterreadingsourceid,
       'VALID' as meterreadingstatusid,
       'ACTUAL' as meterreadingtypeid,
       trim(nosi.meterserialnumber) as meterreadingserialnumber,
       cast(nosi.lastactualmeterreading as double precision) as readingvalue,
       trim(reg.registers_registerreference) as meterregisterreference,
       cast(0 as boolean) as required
FROM ref_meterpoints mp
     inner join aws_s3_stage1_extracts.stage1_readingsnosigas nosi on mp.meterpointnumber = nosi.meterpointreference and mp.meterpointtype ='G' and
         dateadd(day,1,cast(to_date(nosi.confirmationenddate, 'DD/MM/YYYY') as timestamp)) = mp.supplystartdate
     inner join ref_meters mt on mp.account_id = mt.account_id and mp.meter_point_id = mt.meter_point_id and nosi.meterserialnumber = mt.meterserialnumber
     inner join ref_registers reg on mt.account_id = reg.account_id  and mt.meter_id = reg.meter_id
where nosi.lastactualmeterreadingdate <> 'NULL';
--where nosi.meterpointreference = 1494891702


select cast(mp.account_id as bigint) as account_id,
       cast(mp.meter_point_id as bigint) as meter_point_id,
       cast(mt.meter_id as bigint) as meter_id,
       cast(-1 as bigint) as meter_reading_id,
       cast(reg.register_id as bigint) as register_id,
       cast(-1 as bigint) as register_reading_id,
       cast(0 as boolean) as billable,
       cast(0 as boolean) as haslivecharge,
       cast(0 as boolean)as hasregisteradvance,
       cast(nosi.meterpointreference as bigint) as meterpointnumber,
       trim(mp.meterpointtype) as meterpointtype,
       cast(to_date(nosi.lastactualmeterreadingdate, 'DD/MM/YYYY') as timestamp) as meterreadingcreateddate,
       cast(to_date(nosi.lastactualmeterreadingdate, 'DD/MM/YYYY') as timestamp) as metereadingdate,
       'NOSI' as meterreadingsourceid,
       'VALID' as meterreadingstatusid,
       'ACTUAL' as meterreadingtypeid,
       trim(nosi.meterserialnumber) as meterreadingserialnumber,
       cast(nosi.lastactualmeterreading as double precision) as readingvalue,
       trim(reg.registers_registerreference) as meterregisterreference,
       cast(0 as boolean) as required,
            current_timestamp as etlchange
     FROM ref_meterpoints mp
     inner join aws_s3_stage1_extracts.stage1_readingsnosigas nosi on mp.meterpointnumber = nosi.meterpointreference and mp.meterpointtype ='G' and
         dateadd(day,1,cast(to_date(nosi.confirmationenddate, 'DD/MM/YYYY') as timestamp)) = mp.supplystartdate
     inner join ref_meters mt on mp.account_id = mt.account_id and mp.meter_point_id = mt.meter_point_id and nosi.meterserialnumber = mt.meterserialnumber
     inner join ref_registers reg on mt.account_id = reg.account_id  and mt.meter_id = reg.meter_id
              left outer join temp_readings_internal_nosi r
                    on r.account_id = cast (mp.account_id as bigint)
                    and r.meter_point_id = cast (mp.meter_point_id as bigint)
                    and r.meter_id = cast (mt.meter_id as bigint)
                            and r.register_id = cast (reg.register_id as bigint)
                            and r.meterreadingcreateddate = cast(to_date(nosi.lastactualmeterreadingdate, 'DD/MM/YYYY') as timestamp)
        where  cast(nosi.lastactualmeterreading as double precision) !=  r.readingvalue
        and  nosi.lastactualmeterreadingdate <> 'NULL';

insert into ref_readings_internal_nosi
select account_id,
       meter_point_id,
       meter_id,
       meter_reading_id,
       register_id,
       register_reading_id,
       billable,
       haslivecharge,
       hasregisteradvance,
       meterpointnumber,
       meterpointtype,
       meterreadingcreateddate,
       metereadingdate,
       meterreadingsourceid,
       meterreadingstatusid,
       meterreadingtypeid,
       meterreadingserialnumber,
       cast(readingvalue as double precision) as  readingvalue,
       meterregisterreference,
       required
from (SELECT cast(mp.account_id as bigint)                                       as account_id,
             cast(mp.meter_point_id as bigint)                                   as meter_point_id,
             cast(mt.meter_id as bigint)                                         as meter_id,
             cast(-1 as bigint)                                                  as meter_reading_id,
             cast(reg.register_id as bigint)                                     as register_id,
             cast(-1 as bigint)                                                  as register_reading_id,
             cast(0 as boolean)                                                  as billable,
             cast(0 as boolean)                                                  as haslivecharge,
             cast(0 as boolean)as                                                   hasregisteradvance,
             cast(replace(trim(nosi.meterpointreference), 'NULL', '-1') as bigint) as meterpointnumber,
             trim(mp.meterpointtype)                                             as meterpointtype,
             cast(to_date(replace(nosi.lastactualmeterreadingdate, 'NULL', '01/01/1900'), 'DD/MM/YYYY') as
                  timestamp)                                                     as meterreadingcreateddate,
             cast(to_date(replace(nosi.lastactualmeterreadingdate, 'NULL', '01/01/1900'), 'DD/MM/YYYY') as
                  timestamp)                                                     as metereadingdate,
             'NOSI'                                                              as meterreadingsourceid,
             'VALID'                                                             as meterreadingstatusid,
             'ACTUAL'                                                            as meterreadingtypeid,
             replace(trim(nosi.meterserialnumber), 'NULL', '-1')                   as meterreadingserialnumber,
             case
               when replace(trim(nosi.lastactualmeterreading), 'NULL', '-1') is not null
                       then replace(trim(nosi.lastactualmeterreading), 'NULL', '-1')
                 end                                                             as readingvalue,
             trim(reg.registers_registerreference)                               as meterregisterreference,
             cast(0 as boolean)                                                  as required
      FROM ref_meterpoints mp
             inner join aws_s3_stage2_extracts.stage2_readingsnosigas nosi on mp.meterpointnumber =
                                                                              replace(trim(nosi.meterpointreference), 'NULL', '-1') and
                                                                              mp.meterpointtype = 'G'
                                                                                and dateadd(day, 1, cast(to_date(
                                                                                                           replace(trim(nosi.confirmationenddate), '', '01/01/1900'),
                                                                                                           'DD/MM/YYYY')
                                                                                                         as
                                                                                                         timestamp)) =
                                                                                    mp.supplystartdate
             inner join ref_meters mt on mp.account_id = mt.account_id and mp.meter_point_id = mt.meter_point_id and
                                         replace(trim(nosi.meterserialnumber), 'NULL', '-1') = mt.meterserialnumber
             inner join ref_registers reg on mt.account_id = reg.account_id and mt.meter_id = reg.meter_id)
--order by cast(mp.account_id as bigint)
             -- limit 22



select count(*) from aws_s3_stage1_extracts.stage1_readingsnosigas nosi
--where nosi.lastactualmeterreadingdate <> 'NULL';
select count(*) from aws_s3_stage2_extracts.stage2_readingsnosigas nosi
select count(*) from temp_readings_internal_nosi;


select account_id,
       meter_point_id,
       meter_id,
       meter_reading_id,
       register_id,
       register_reading_id,
       billable,
       haslivecharge,
       hasregisteradvance,
       meterpointnumber,
       meterpointtype,
       meterreadingcreateddate,
       metereadingdate,
       meterreadingsourceid,
       meterreadingstatusid,
       meterreadingtypeid,
       meterreadingserialnumber,
       cast(readingvalue as double precision) as readingvalue,
       meterregisterreference,
       required
from (SELECT cast(mp.account_id as bigint)                                   as account_id,
             cast(mp.meter_point_id as bigint)                               as meter_point_id,
             cast(mt.meter_id as bigint)                                     as meter_id,
             cast(-1 as bigint)                                              as meter_reading_id,
             cast(reg.register_id as bigint)                                 as register_id,
             cast(-1 as bigint)                                              as register_reading_id,
             cast(0 as boolean)                                              as billable,
             cast(0 as boolean)                                              as haslivecharge,
             cast(0 as boolean)                                              as hasregisteradvance,
             cast(nosi.meterpointreference as bigint)                        as meterpointnumber,
             trim(mp.meterpointtype)                                         as meterpointtype,
             cast((case
                     when nosi.lastactualmeterreadingdate = '' then null
                     else nosi.lastactualmeterreadingdate end) as timestamp) as meterreadingcreateddate,
             cast((case
                     when nosi.lastactualmeterreadingdate = '' then null
                     else nosi.lastactualmeterreadingdate end) as timestamp) as metereadingdate,
             'NOSI'                                                          as meterreadingsourceid,
             'VALID'                                                         as meterreadingstatusid,
             'ACTUAL'                                                        as meterreadingtypeid,
             replace(trim(nosi.meterserialnumber), '', '')                   as meterreadingserialnumber,
             case
               when replace(trim(nosi.lastactualmeterreading), '', '') is not null
                       then replace(trim(nosi.lastactualmeterreading), '', '')
                 end                                                         as readingvalue,
             trim(reg.registers_registerreference)                           as meterregisterreference,
             cast(0 as boolean)                                              as required
      FROM aws_s3_stage1_extracts.stage1_readingsnosigas nosi
             inner join ref_meterpoints mp on mp.meterpointnumber = replace(trim(nosi.meterpointreference), '', '') and
                                              mp.meterpointtype = 'G'
                                                and dateadd(day, 1, cast((case
                                                                            when nosi.lastactualmeterreadingdate = ''
                                                                                    then null
                                                                            else nosi.lastactualmeterreadingdate end) as
                                                                         timestamp)) =
                                                    mp.supplystartdate
             inner join ref_meters mt on mp.account_id = mt.account_id and mp.meter_point_id = mt.meter_point_id and
                                         replace(trim(nosi.meterserialnumber), '', '') = mt.meterserialnumber
             inner join ref_registers reg on mt.account_id = reg.account_id and mt.meter_id = reg.meter_id
      group by cast(mp.account_id as bigint),
               cast(mp.meter_point_id as bigint),
               cast(mt.meter_id as bigint),
               cast(reg.register_id as bigint),
               cast(nosi.meterpointreference as bigint),
               trim(mp.meterpointtype),
               cast((case
                       when nosi.lastactualmeterreadingdate = '' then null
                       else nosi.lastactualmeterreadingdate end) as timestamp),
               cast((case
                       when nosi.lastactualmeterreadingdate = '' then null
                       else nosi.lastactualmeterreadingdate end) as timestamp),
               replace(trim(nosi.meterserialnumber), '', ''),
               case
                 when replace(trim(nosi.lastactualmeterreading), '', '') is not null
                         then replace(trim(nosi.lastactualmeterreading), '', '')
                   end,
               trim(reg.registers_registerreference))


