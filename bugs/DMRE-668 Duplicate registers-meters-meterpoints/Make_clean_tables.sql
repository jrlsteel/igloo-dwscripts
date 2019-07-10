-- Make raw tables from originals
-- ref_meterpoints_raw
create table temp_ref_meterpoints_raw as
select account_id,
       meter_point_id,
       meterpointnumber,
       associationstartdate,
       associationenddate,
       supplystartdate,
       supplyenddate,
       issmart,
       issmartcommunicating,
       meterpointtype,
       -- usage flag
       case
           when duplicate_num > 1 then 'duplicate'
           else
               case
                   when cancelled_flag then 'cancelled'
                   else
                       case
                           when et_flag then
                               case
                                   when start_date = min(start_date) over (partition by account_id, meterpointnumber)
                                       then 'etloss_start'
                                   else 'etloss_end'
                                   end
                           else 'valid' end end end as usage_flag,
       end_date,
       start_date
from (select *,
             row_number()
             over (partition by account_id, meter_point_id, meterpointnumber, associationstartdate, associationenddate,
                 supplystartdate, supplyenddate, issmart, issmartcommunicating, meterpointtype) > 1 as duplicate_num,
             least(associationenddate, supplyenddate)                                               as end_date,
             greatest(supplystartdate, associationstartdate)                                        as start_date,
             least(associationenddate, supplyenddate) <
             greatest(supplystartdate, associationstartdate) and
             nvl(associationenddate, supplyenddate) notnull                                         as cancelled_flag,
             min(least(associationenddate, supplyenddate)) over (partition by account_id, meterpointnumber) <
             max(greatest(supplystartdate, associationstartdate))
             over (partition by account_id, meterpointnumber)                                       as et_flag
      from ref_meterpoints
     ) mps_multiflag
;

--ref_meters_raw
create table temp_ref_meters_raw as
select account_id,
       meter_point_id,
       meter_id,
       meterserialnumber,
       installeddate,
       removeddate,
       case
           when row_number()
                over (partition by account_id, meter_point_id, meter_id, meterserialnumber, installeddate, removeddate) >
                1 then 'duplicate'
           else 'valid' end as usage_flag
from ref_meters;

create table temp_ref_registers_raw as
--ref_registers_raw
select account_id,
       meter_point_id,
       meter_id,
       register_id,
       registers_eacaq,
       registers_registerreference,
       registers_sourceidtype,
       registers_tariffcomponent,
       registers_tpr,
       registers_tprperioddescription,
       case
           when row_number()
                over (partition by account_id,
                    meter_point_id,
                    meter_id,
                    register_id,
                    registers_eacaq,
                    registers_registerreference,
                    registers_sourceidtype,
                    registers_tariffcomponent,
                    registers_tpr,
                    registers_tprperioddescription) >
                1 then 'duplicate'
           else 'valid' end as usage_flag
from ref_registers;

--ref_meterpoints_attributes_raw
create table temp_ref_meterpoints_attributes_raw as
select account_id,
       meter_point_id,
       attributes_attributename,
       attributes_attributedescription,
       attributes_attributevalue,
       attributes_effectivefromdate,
       attributes_effectivetodate,
       case
           when row_number()
                over (partition by account_id,
                    meter_point_id,
                    attributes_attributename,
                    attributes_attributedescription,
                    attributes_attributevalue,
                    attributes_effectivefromdate,
                    attributes_effectivetodate) >
                1 then 'duplicate'
           else 'valid' end as usage_flag
from ref_meterpoints_attributes;

--ref_meters_attributes_raw
create table temp_ref_meters_attributes_raw as
select account_id,
       meter_point_id,
       meter_id,
       metersattributes_attributename,
       metersattributes_attributedescription,
       metersattributes_attributevalue,
       case
           when row_number()
                over (partition by account_id,
                    meter_point_id,
                    meter_id,
                    metersattributes_attributename,
                    metersattributes_attributedescription,
                    metersattributes_attributevalue) >
                1 then 'duplicate'
           else 'valid' end as usage_flag
from ref_meters_attributes;

--ref_registers_attributes_raw
create table temp_ref_registers_attributes_raw as
select account_id,
       meter_point_id,
       meter_id,
       register_id,
       registersattributes_attributename,
       registersattributes_attributedescription,
       registersattributes_attributevalue,
       case
           when row_number()
                over (partition by account_id,
                    meter_point_id,
                    meter_id,
                    register_id,
                    registersattributes_attributename,
                    registersattributes_attributedescription,
                    registersattributes_attributevalue) >
                1 then 'duplicate'
           else 'valid' end as usage_flag
from ref_registers_attributes;


-- create clean tables
--ref_meterpoints
create table temp_ref_meterpoints as
select account_id,
       meter_point_id,
       meterpointnumber,
       min(associationstartdate)                                                as associationstartdate,
       nullif(max(nvl(associationenddate, getdate() + 1000)), getdate() + 1000) as associationenddate,
       min(supplystartdate)                                                     as supplystartdate,
       nullif(max(nvl(supplyenddate, getdate() + 1000)), getdate() + 1000)      as supplyenddate,
       issmart,
       issmartcommunicating,
       meterpointtype
from temp_ref_meterpoints_raw
where usage_flag in ('valid', 'etloss_start', 'etloss_end')
group by account_id, meter_point_id, meterpointnumber, issmart, issmartcommunicating, meterpointtype;

--ref_meters
create table temp_ref_meters as
select account_id, meter_point_id, meter_id, meterserialnumber, installeddate, removeddate
from temp_ref_meters_raw
where usage_flag = 'valid';

--ref_registers
create table temp_ref_registers as
select account_id,
       meter_point_id,
       meter_id,
       register_id,
       registers_eacaq,
       registers_registerreference,
       registers_sourceidtype,
       registers_tariffcomponent,
       registers_tpr,
       registers_tprperioddescription
from temp_ref_registers_raw
where usage_flag = 'valid';

--ref_meterpoints_attributes
create table temp_ref_meterpoints_attributes as
select account_id,
       meter_point_id,
       attributes_attributename,
       attributes_attributedescription,
       attributes_attributevalue,
       attributes_effectivefromdate,
       attributes_effectivetodate
from temp_ref_meterpoints_attributes_raw
where usage_flag = 'valid';

--ref_meters_attributes
create table temp_ref_meters_attributes as
select account_id,
       meter_point_id,
       meter_id,
       metersattributes_attributename,
       metersattributes_attributedescription,
       metersattributes_attributevalue
from temp_ref_meters_attributes_raw
where usage_flag = 'valid';

--ref_registers_attributes
create table temp_ref_registers_attributes as
select account_id,
       meter_point_id,
       meter_id,
       register_id,
       registersattributes_attributename,
       registersattributes_attributedescription,
       registersattributes_attributevalue
from temp_ref_registers_attributes_raw
where usage_flag = 'valid';