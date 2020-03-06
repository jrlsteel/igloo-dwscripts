select distinct(attributes_attributename)
from ref_meterpoints_attributes


select distinct(metersattributes_attributename)
from ref_meters_attributes


select count(*) from ref_meters_attributes mta_elec
where mta_elec.metersattributes_attributename = 'Year_Of_Manufacture'


select distinct(attributes_attributename)
from ref_meterpoints_attributes


select * from ref_meterpoints
where account_id = 1831


select * from ref_meters
where account_id = 1831



select * from ref_meterpoints_attributes
where account_id = 1831


select *
from ref_meters_attributes
where account_id = 1831


select * from ref_registers
where account_id = 1831

select * from ref_registers_attributes
where account_id = 1831

select * from aws_met_stage1_extracts.met_igloo_smart_metering_estate_firmware smef
where smef."mpxn number" =  2000007794886


2000020479229


select * from ref_meterpoints
where account_id =84505

select * from ref_meters
where account_id = 84505


select * from ref_meterpoints_attributes
where account_id = 84505


select *
from ref_meters_attributes
where account_id = 84505

select * from aws_met_stage1_extracts.met_igloo_smart_metering_estate_firmware smef
where smef."mpxn number" =  2000020479229