select su.external_id               as supply_conatract_external_id,
       ac.status                    as account_status,
       reg_elec_status.status       as mp_elec_reg_status,
       reg_status_gas.status        as mp_gas_reg_status,
       mp_elec.account_id           as mp_elec_account_id,
       mp_elec.meter_point_id       as mp_elec_meter_point_id,
       mp_elec.supplystartdate      as mp_elec_ssd,
       mp_elec.supplyenddate        as mp_elec_sed,
       mp_elec.associationstartdate as mp_elec_asd,
       mp_elec.associationenddate   as mp_elec_aed,
       mp_gas.account_id            as mp_gas_account_id,
       mp_gas.meter_point_id        as mp_elec_meter_point_id,
       mp_gas.supplystartdate       as mp_gas_ssd,
       mp_gas.supplyenddate         as mp_gas_sed,
       mp_gas.associationstartdate  as mp_gas_asd,
       mp_gas.associationenddate    as mp_gas_aed
from ref_cdb_supply_contracts su
       left outer join ref_account_status ac on su.external_id = ac.account_id
       left outer join ref_meterpoints mp_elec on mp_elec.account_id = su.external_id and mp_elec.meterpointtype = 'E'
       left outer join ref_registrations_meterpoints_status_elec reg_elec_status
         on mp_elec.account_id = reg_elec_status.account_id and mp_elec.meter_point_id = reg_elec_status.meterpoint_id
       left outer join ref_meters mt_elec
         on mp_elec.account_id = mt_elec.account_id and mp_elec.meter_point_id = mt_elec.meter_point_id and
            mt_elec.removeddate is null --left outer join ref_meters_attributes mta_elec on mt_elec.meter_id = mta_elec.meter_id
       left outer join ref_registers reg_elec
         on mt_elec.account_id = reg_elec.account_id and mt_elec.meter_id = reg_elec.meter_id --Ensek Meterpoint Gas
       left outer join ref_meterpoints mp_gas on mp_gas.account_id = su.external_id and mp_gas.meterpointtype = 'G'
       left outer join ref_registrations_meterpoints_status_gas reg_status_gas
         on mp_gas.account_id = reg_status_gas.account_id and mp_gas.meter_point_id = reg_status_gas.meterpoint_id
       left outer join ref_meters mt_gas
         on mp_gas.account_id = mt_gas.account_id and mp_gas.meter_point_id = mt_gas.meter_point_id and
            mt_gas.removeddate is null
       left outer join ref_registers reg_gas on mt_gas.account_id = reg_gas.account_id and
                                                mt_gas.meter_id = reg_gas.meter_id --left outer join ref_re