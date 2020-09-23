-- drop table ref_calculated_metering_portfolio_gas_report;
-- create table ref_calculated_metering_portfolio_gas_report as
drop table temp_metering_gas;
create table temp_metering_gas as
with cte_mpa as (
    select account_id, meter_point_id, attributes_attributename as att_name, max(attributes_attributevalue) as att_value
    from ref_meterpoints_attributes
    where attributes_effectivetodate is null
    group by account_id, meter_point_id, attributes_attributename
),
     cte_ma as (
         select account_id,
                meter_point_id,
                meter_id,
                metersattributes_attributename       as att_name,
                max(metersattributes_attributevalue) as att_value
         from ref_meters_attributes
         group by account_id, meter_point_id, meter_id, metersattributes_attributename
     )
select state.account_id,
       left(postcode, len(postcode) - 3)                        as Postcode,
       state.acc_stat,
       dcf.supply_type,
       vmrc.gain_type,
       vmrc.gain_date,
       vmrc.loss_type,
       vmrc.loss_date,
       case
           when (mt_gas.installeddate > dateadd(day, 2, mp_gas.supplystartdate)) and
                mt_gas.installeddate > '2019-08-01 00:00:00.000000' then 'IglooInstalled'
           else 'NonIglooInstalled' end                         as InstalledBy,
       mp_gas.supplystartdate,
       mp_gas.associationstartdate                              as accmeterpointstartdate,
       state.aed                                                as associationenddate,
       state.sed                                                as supplyenddate,
       state.home_move_in,
       vmrrs.attribute_value                                    as data_consent,
       nullif(vmrp.psr, '')                                     as psr,
       case when dcf.occupier_account then 'PORB' else 'DD' end as payment_type,
       th.tariff_name,
       mp_gas.meter_point_id,
       mp_gas.meterpointnumber                                  as mprn,
       mp_gas.meterpointtype,
       rsi.dspinventory_gsme_deviceid                           as device_id, -- This is NOT the device ID listed in smart readings
--        rsi.firmware_version,
       rsi.dspinventory_gsme_devicefirmwareversion              as firmware_version,
--        rsi.manufacturer,
       rsi.dspinventory_gsme_devicemanufacturer                 as manufacturer,
--        rsi.type,
       rsi.dspinventory_gsme_devicetype                         as type,
--        rsi.device_status,
       rsi.dspinventory_gsme_devicestatus                       as device_status,
--        rsi.commisioned_date,
       rsi.dspinventory_gsme_datecommissioned                   as commisioned_date,
--        rsi.tariff,
       null                                                     as tariff,    -- not supplied in the inventory file. This should be what the meter "thinks" the tariff is
       accs.billdayofmonth,
       accs.nextbilldate,
       mt_gas.meter_id,
       mt_gas.meterserialnumber,
       mt_gas.installeddate,
       reg_elec.register_id,
       vmri.meterreadingstatusuid,
       vmri.meterreadingtypeuid,
       vmri.meterreadingsourceuid,
       vmri.meterreadingdatetime,
       mpa_ldz.att_value                                        as mpa_gas_LDZ,
       mpa_gmlc.att_value                                       as mta_gas_meter_loc_code,
       replace(replace(mpa_mmm.att_value, ',', ' '), '"', ' ')  as mpa_gas_meter_make_model,
       ma_mmc.att_value                                         as mta_gas_meter_type,
       ma_yom.att_value                                         as mta_gas_meter_year_manu,
       ma_manc.att_value                                        as mta_gas_meter_manu_code,
       ma_modc.att_value                                        as mta_gas_meter_model_code,
       getdate()                                                as etlchange
from vw_meterpoint_live_state state
         inner join ref_meterpoints mp_gas on state.account_id = mp_gas.account_id and
                                              mp_gas.meterpointtype = 'G'
         left join ref_meters mt_gas
                   on mp_gas.account_id = mt_gas.account_id and mp_gas.meter_point_id = mt_gas.meter_point_id and
                      mt_gas.removeddate is null
         left join cte_mpa mpa_ldz
                   on mpa_ldz.account_id = mp_gas.account_id and mpa_ldz.meter_point_id = mp_gas.meter_point_id and
                      mpa_ldz.att_name = 'LDZ'
         left join cte_mpa mpa_gmlc
                   on mpa_gmlc.account_id = mp_gas.account_id and mpa_gmlc.meter_point_id = mp_gas.meter_point_id and
                      mpa_gmlc.att_name = 'Gas_Meter_Location_Code'
         left join cte_mpa mpa_mmm
                   on mpa_mmm.account_id = mp_gas.account_id and mpa_mmm.meter_point_id = mp_gas.meter_point_id and
                      mpa_mmm.att_name = 'MeterMakeAndModel'
         left join cte_ma ma_mmc
                   on ma_mmc.account_id = mp_gas.account_id and ma_mmc.meter_point_id = mp_gas.meter_point_id and
                      ma_mmc.meter_id = mt_gas.meter_id and ma_mmc.att_name = 'Meter_Mechanism_Code'
         left join cte_ma ma_yom
                   on ma_yom.account_id = mp_gas.account_id and ma_yom.meter_point_id = mp_gas.meter_point_id and
                      ma_yom.meter_id = mt_gas.meter_id and ma_yom.att_name = 'Year_Of_Manufacture'
         left join cte_ma ma_manc
                   on ma_manc.account_id = mp_gas.account_id and ma_manc.meter_point_id = mp_gas.meter_point_id and
                      ma_manc.meter_id = mt_gas.meter_id and ma_manc.att_name = 'Manufacture_Code'
         left join cte_ma ma_modc
                   on ma_modc.account_id = mp_gas.account_id and ma_modc.meter_point_id = mp_gas.meter_point_id and
                      ma_modc.meter_id = mt_gas.meter_id and ma_modc.att_name = 'Model_Code'
         left join ref_registers reg_elec
                   on mt_gas.account_id = reg_elec.account_id and mt_gas.meter_id = reg_elec.meter_id
         left join aws_s3_stage2_extracts.stage2_accountsettings accs on mp_gas.account_id = accs.account_id
         left join vw_metering_report_reads_info vmri
                   on mp_gas.account_id = vmri.account_id and reg_elec.register_id = vmri.register_id
         left join ref_smart_inventory rsi
                   on mp_gas.meterpointnumber = rsi.dspinventory_gsme_importmpxn --and smef.device_status <> 'InstalledNotCommissioned'
         left join vw_supply_contracts_with_occ_accs vscoa on mp_gas.account_id = vscoa.external_id
         left join ref_cdb_addresses rca on vscoa.supply_address_id = rca.id
         left join vw_metering_report_read_schedule vmrrs on vscoa.external_id = vmrrs.external_id
         left join ref_tariff_history th on vscoa.external_id = th.account_id and th.end_date is null
         left join vw_metering_report_psr vmrp on vscoa.external_id = vmrp.external_id
         left join vw_metering_report_cot vmrc
                   on state.account_id = vmrc.account_id and vmrc.meter_point_id = mp_gas.meter_point_id
         left join ref_calculated_daily_customer_file dcf on state.account_id = dcf.account_id
order by mp_gas.account_id, mp_gas.meter_point_id, mp_gas.supplystartdate, mt_gas.meter_id


