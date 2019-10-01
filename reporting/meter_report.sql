select mp.meterpointnumber                                         as MPR,
       met.meterserialnumber                                       as MSN,
       met.account_id                                              as Account_ID,
       mp.meterpointtype                                           as fuel_type,
       mp.supplystartdate                                          as meterpoint_SSD,
       mp.supplyenddate                                            as meterpoint_SED,
       udf_meterpoint_status(mp.supplystartdate, mp.supplyenddate) as meterpoint_status,
       greatest(mp.supplystartdate, mp.associationstartdate)       as acc_mp_SSD,
       least(mp.supplyenddate, mp.associationenddate)              as acc_mp_SED,
       udf_meterpoint_status(acc_mp_SSD, acc_mp_SED)               as acc_mp_status,
       rma_status.metersattributes_attributevalue                  as meter_status,
       met.installeddate                                           as meter_install_date,
       met.removeddate                                             as meter_removed_date,
       rma_type.metersattributes_attributevalue                    as meter_type,
       rma_location.metersattributes_attributevalue                as meter_location,
       rma_mech.metersattributes_attributevalue                    as meter_mechanism,
       num_reg.reg_count                                           as num_registers,
       rma_digits.attributes_attributevalue                        as num_dials,
       rma_ssc.attributes_attributevalue                           as SSC,
       rma_mopmam.attributes_attributevalue                        as MOP_MAM,
       rma_mopmam.attributes_effectivefromdate                     as MOP_MAM_effective_date,
--        null                                                                  as MAM,
--        null                                                                  as MAM_effective_date,
       old_mopmams.old_mopmams                                     as old_MOP_MAM,
--        null                                                                  as old_MAM,
       rma_osmopmam.attributes_attributevalue                      as old_supplier_MOP_MAM,
--        null                                                                  as old_supplier_MAM,
--        case
--            when count(met_repl) > 0 then 'Yes'
--            when met.removeddate is not null and met.removeddate < getdate() then 'Removed'
--            else 'No'
--            end                                                     as MEX_occurred,
--        met_repl.installeddate                                      as MEX_date,
       null                                                        as F_read,
       null                                                        as F_read_date,
       null                                                        as I_read,
       null                                                        as I_read_date,
       null                                                        as SSD_DC_read_in,
       null                                                        as SSD_DC_read_date,
       null                                                        as EAC_in,
       null                                                        as EAC_effective_date,
       null                                                        as AQ_in,
       null                                                        as AQ_effective_date,
       null                                                        as Final_DC_Read_in,
       null                                                        as Final_DC_read_date

from ref_meters met
         left join ref_meterpoints mp on met.meter_point_id = mp.meter_point_id and met.account_id = mp.account_id
         left join ref_meters_attributes rma_status on rma_status.metersattributes_attributename = 'Meter_Status' and
                                                       rma_status.meter_id = met.meter_id and
                                                       rma_status.account_id = met.account_id
         left join ref_meters_attributes rma_type on rma_type.metersattributes_attributename = 'MeterType' and
                                                     rma_type.meter_id = met.meter_id and
                                                     rma_type.account_id = met.account_id
         left join ref_meters_attributes rma_location
                   on rma_location.metersattributes_attributename = 'Meter_Location' and
                      rma_location.meter_id = met.meter_id and
                      rma_location.account_id = met.account_id
         left join ref_meters_attributes rma_mech on rma_mech.metersattributes_attributename = 'Gas_Meter_Mechanism' and
                                                     rma_mech.meter_id = met.meter_id and
                                                     rma_mech.account_id = met.account_id
         left join (select account_id, meter_id, count(distinct register_id) as reg_count
                    from ref_registers
                    group by account_id, meter_id) num_reg on num_reg.meter_id = met.meter_id and
                                                              num_reg.account_id = met.account_id
         left join ref_meterpoints_attributes rma_digits
                   on rma_digits.attributes_attributename in ('No_Of_Digits', 'Gas_No_Of_Digits') and
                      rma_digits.meter_point_id = met.meter_point_id and
                      rma_digits.account_id = met.account_id
         left join ref_meterpoints_attributes rma_ssc on rma_ssc.attributes_attributename = 'SSC' and
                                                         rma_ssc.meter_point_id = met.meter_point_id and
                                                         rma_ssc.account_id = met.account_id
         left join ref_meterpoints_attributes rma_mopmam on rma_mopmam.attributes_attributename in ('MOP', 'MAM') and
                                                            rma_mopmam.meter_point_id = met.meter_point_id and
                                                            rma_mopmam.account_id = met.account_id and
                                                            (rma_mopmam.attributes_effectivetodate is null or
                                                             rma_mopmam.attributes_effectivetodate > getdate())
         left join (select account_id, meter_point_id, listagg(distinct attributes_attributevalue) as old_mopmams
                    from ref_meterpoints_attributes
                    where attributes_attributename in ('MOP', 'MAM')
                      and attributes_effectivetodate is not null
                      and attributes_effectivetodate < getdate()
                    group by account_id, meter_point_id) old_mopmams on old_mopmams.account_id = met.account_id and
                                                                        old_mopmams.meter_point_id = met.meter_point_id
         left join ref_meterpoints_attributes rma_osmopmam
                   on rma_osmopmam.attributes_attributename in ('OLD_SUPPLIER_MOP', 'OLD_SUPPLIER_MAM') and
                      rma_osmopmam.account_id = met.account_id and rma_osmopmam.meter_point_id = met.meter_point_id
         /*left join ref_meters met_repl
                   on met_repl.account_id = met.account_id and met_repl.meter_point_id = met.meter_point_id and
                      datediff(days, met.removeddate, met_repl.installeddate) between 0 and 5
*/

order by mpr, msn, Account_ID

select distinct attributes_attributename
from ref_meterpoints_attributes
select *
from ref_meters_attributes
/*
 metersattributes_attributename
MeterType
Gas_Meter_Mechanism
Manufacture_Code
Meter_Mechanism_Code
Year_Of_Manufacture
Meter_Location
Manufacturers_Make_Type
Bypass_Fitted_Indicator
Collar_Fitted_Indicator
Conversion_Factor
Amr_Indicator
Gas_Act_Owner
Meter_Link_Code
Meter_Status
Model_Code
Payment_Method_Code
Meter_Location_Description
METER_LOCATION
Imperial_Indicator
Inspection_Date
Meter_Manufacturer_Code
Measuring_Capacity
Meter_Reading_Factor
Pulse_Value

 */


/* meterpoints attributes attributes_attributename
BILLING STATUS
CLIENT_UNIQUE_REFERENCE
Confirmation_Reference
Current_Mam_Abbreviated_Name
DA
DC
DisconnectionDate
Disconnection_Status
Distributor
ET
EnergisationStatus
GAIN_SUPPLIER
GSP
Gas_Act_Owner
Gas_Imperial_Meter_Indicator
Gas_Loss_Confirmation_Reference
Gas_Meter_Location_Code
Gas_Meter_Manufactured_Year
Gas_Meter_Manufacturer_Code
Gas_Meter_Mechanism
Gas_Meter_Model
Gas_Meter_Serial_Number
Gas_Meter_Status
Gas_No_Of_Digits
Green Deal
IGT Indicator
IsPrepay
LDZ
LLF Indicator
LLFC
LOSS_REGISTRATION_TRANSACTION_NUMBER
Large Site Indicator
Location_Code
Loss Objection
MAM
MOP
MTC
MTC Related
Measurement Class
Meter Designation
Meter Status
MeterMakeAndModel
MeterType
Meter_Point_Status
Meter_Round_The_Clock_Count
Metering Type
NEW_DA
NEW_DC
NEW_MAM
NEW_MOP
NOMINATION_SHIPPER_REF
No_Of_Digits
OLD_DA
OLD_DC
OLD_MAM
OLD_MOP
OLD_SUPPLIER
OLD_SUPPLIER_DA
OLD_SUPPLIER_DC
OLD_SUPPLIER_MAM
OLD_SUPPLIER_MOP
Objection Status
Profile Class
ReadCycle
Registration_Transaction_Number
SSC
SUPPLIER
SUPPLY_POINT_CATEGORY
Supply_Status
Threshold.DailyConsumption
Transporter
greenDealActive
igtIndicator
isCOT
isPrepay

*/

select meterpointnumber
from ref_meterpoints
group by meterpointnumber
having count(distinct supplyenddate) > 1

select *
from ref_meterpoints
where meterpointnumber = 1200061643950
select *
from ref_meters

select distinct metersattributes_attributevalue
from ref_meters_attributes
where metersattributes_attributename = 'MeterType'

select account_id, meter_point_id
from ref_meterpoints_attributes
where attributes_attributename = 'MOP'
group by account_id, meter_point_id
having count(*) > 2

select *
from ref_meterpoints_attributes
where meter_point_id = 26884
  and attributes_attributename = 'MOP'

select meter_id
from ref_meters
group by meter_id
having count(removeddate) > 0-- and count(removeddate) < count(*)

select *
from ref_meters
where account_id = 1831