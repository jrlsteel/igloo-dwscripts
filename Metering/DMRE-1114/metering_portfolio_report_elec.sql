-- drop table ref_calculated_metering_portfolio_elec_report
-- create table ref_calculated_metering_portfolio_elec_report as
-- create table temp_metering_elec as
--     ;
-- truncate table temp_metering_elec;
-- insert into temp_metering_elec
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
           when (mt_elec.installeddate > dateadd(day, 2, mp_elec.supplystartdate)) and
                mt_elec.installeddate > '2019-08-01 00:00:00.000000' then 'IglooInstalled'
           else 'NonIglooInstalled' end                         as InstalledBy,
       mp_elec.supplystartdate,
       mp_elec.associationstartdate                             as accmeterpointstartdate,
       state.aed                                                as associationenddate,
       state.sed                                                as supplyenddate,
       state.home_move_in,
       vmrrs.attribute_value                                    as data_consent,
       nullif(vmrp.psr, '')                                     as psr,
       case when dcf.occupier_account then 'PORB' else 'DD' end as payment_type,
       th.tariff_name,
       mp_elec.meter_point_id,
       mp_elec.meterpointnumber                                 as mpan,
       mp_elec.meterpointtype,
       rsi.deviceid,
--        rsi.firmware_version,
       rsi.dspinventory_esme_devicefirmwareversion              as firmware_version,
--        rsi.manufacturer,
       rsi.dspinventory_esme_devicemanufacturer                 as manufacturer,
--        rsi.type,
       rsi.dspinventory_esme_devicetype                         as type,
--        rsi.device_status,
       rsi.dspinventory_esme_devicestatus                       as device_status,
--        rsi.commisioned_date,
       rsi.dspinventory_esme_datecommissioned                   as commisioned_date,
--        rsi.tariff,
       null                                                     as tariff, -- not supplied in the inventory file. This should be what the meter "thinks" the tariff is
       accs.billdayofmonth,
       accs.nextbilldate,
       mt_elec.meter_id,
       mt_elec.meterserialnumber,
       mt_elec.installeddate,
       reg_elec.register_id,
       reg_elec.registers_tpr,
       vmri.meterreadingstatusuid,
       vmri.meterreadingtypeuid,
       vmri.meterreadingsourceuid,
       vmri.meterreadingdatetime,
       mpa_pc.att_value                                         as mpa_elec_profile_class,
       mpa_gsp.att_value                                        as mpa_elec_gsp,
       mpa_ssc.att_value                                        as mpa_elec_ssc,
       ma_ml.att_value                                          as mta_elec_meter_loc_code,
       replace(replace(mpa_mmm.att_value, ',', ' '), '"', ' ')  as mpa_elec_meter_make_model,
       ma_mt.att_value                                          as mta_elec_meter_type,
       getdate()                                                as etlchange
from vw_meterpoint_live_state state
         inner join ref_meterpoints mp_elec on state.account_id = mp_elec.account_id and
                                               mp_elec.meterpointtype = 'E'
         left join ref_calculated_daily_customer_file dcf on mp_elec.account_id = dcf.account_id
         left join cte_mpa mpa_pc
                   on mp_elec.account_id = mpa_pc.account_id and
                      mp_elec.meter_point_id = mpa_pc.meter_point_id and
                      mpa_pc.att_name = 'Profile Class'
         left join cte_mpa mpa_gsp
                   on mp_elec.account_id = mpa_gsp.account_id and
                      mp_elec.meter_point_id = mpa_gsp.meter_point_id and
                      mpa_gsp.att_name = 'GSP'
         left join cte_mpa mpa_ssc
                   on mp_elec.account_id = mpa_ssc.account_id and
                      mp_elec.meter_point_id = mpa_ssc.meter_point_id and
                      mpa_ssc.att_name = 'SSC'
         left join cte_mpa mpa_mmm
                   on mp_elec.account_id = mpa_mmm.account_id and
                      mp_elec.meter_point_id = mpa_mmm.meter_point_id and
                      mpa_mmm.att_name = 'MeterMakeAndModel'
         left join ref_meters mt_elec
                   on mp_elec.account_id = mt_elec.account_id and
                      mp_elec.meter_point_id = mt_elec.meter_point_id and
                      mt_elec.removeddate is null
         left join cte_ma ma_ml
                   on mp_elec.account_id = ma_ml.account_id and
                      mt_elec.meter_point_id = ma_ml.meter_point_id and
                      mt_elec.meter_id = ma_ml.meter_id and
                      ma_ml.att_name = 'METER_LOCATION'
         left join cte_ma ma_mt
                   on mp_elec.account_id = ma_mt.account_id and
                      mt_elec.meter_point_id = ma_mt.meter_point_id and
                      mt_elec.meter_id = ma_mt.meter_id and
                      ma_mt.att_name = 'MeterType'
         left join ref_registers reg_elec
                   on mt_elec.account_id = reg_elec.account_id and mt_elec.meter_id = reg_elec.meter_id
         left join aws_s3_stage2_extracts.stage2_accountsettings accs on mp_elec.account_id = accs.account_id
         left join vw_metering_report_reads_info vmri
                   on mp_elec.account_id = vmri.account_id and reg_elec.register_id = vmri.register_id
    --          left join aws_met_stage1_extracts.met_igloo_smart_metering_estate_firmware smef
--                    on mp_elec.meterpointnumber = smef.mpxn_number --and smef.device_status <> 'InstalledNotCommissioned'
         left join ref_smart_inventory rsi
                   on mp_elec.meterpointnumber = rsi.mpxn
         left join vw_supply_contracts_with_occ_accs vscoa on mp_elec.account_id = vscoa.external_id
         left join ref_cdb_addresses rca on vscoa.supply_address_id = rca.id
         left join vw_metering_report_read_schedule vmrrs on vscoa.external_id = vmrrs.external_id
         left join ref_tariff_history th on vscoa.external_id = th.account_id and th.end_date is null
         left join vw_metering_report_psr vmrp on vscoa.external_id = vmrp.external_id
         left join vw_metering_report_cot vmrc
                   on state.account_id = vmrc.account_id and vmrc.meter_point_id = mp_elec.meter_point_id
order by mp_elec.account_id, mp_elec.meter_point_id, mp_elec.supplystartdate, mt_elec.meter_id