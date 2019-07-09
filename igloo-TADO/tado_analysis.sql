select
                          x.account_id as account_id,
                          x.latest_reading_datetime as reading_datetime,
                          max(x.pa_cons_elec_acc) as pa_cons_elec,
                          max(x.igl_ind_eac_acc) as igl_ind_eac,
                          max(x.ind_eac_acc) as ind_eac,
                          cast(max(x.quotes_eac_acc) as double precision) as quotes_eac,
                          getdate() as etlchange
                    from (
                    select
                    reads.external_id as account_id,
                    reads.register_id as register_id,
                    reads.register_reading_id reading_id,
                    reads.meterreadingdatetime as reading_datetime,
                    reads.latest_reading_datetime,
                    reads.latest_read_per_register,
                    reads.prev_reading_datetime,

                    pa_eac.igloo_eac as pa_cons_elec_reg,
                    ig_eac.igl_ind_eac as igl_ind_eac_reg,
                    ee.estimation_value as ind_eac_reg,
                    q.electricity_usage quotes_eac,

                    sum(pa_eac.igloo_eac) over (partition by reads.external_id, reads.latest_read_per_register) as pa_cons_elec_acc,
                    sum(ig_eac.igl_ind_eac) over (partition by reads.external_id, reads.latest_read_per_register) as igl_ind_eac_acc,
                    sum(case when ee.estimation_value is null then prev_ee.estimation_value else ee.estimation_value end) over (partition by reads.external_id, reads.latest_read_per_register) as ind_eac_acc,
                    max(case when q.electricity_usage is null then
                        (q.electricity_projected - ( 3.65 * q.electricity_standing)) / (q.electricity_unit / 100)
                             else q.electricity_usage end) over (partition by reads.external_id, reads.latest_read_per_register) as quotes_eac_acc

                    from (select su.external_id, su.registration_id, mp.meterpointnumber, reg.register_id, ri.register_reading_id, mt.meterserialnumber, reg.registers_eacaq, reg.registers_registerreference, ri.meterreadingdatetime,
                                 max(ri.meterreadingdatetime) over (partition by su.external_id) as latest_reading_datetime,
                                 lead(ri.meterreadingdatetime) over (partition by su.external_id order by ri.meterreadingdatetime desc) as prev_reading_datetime,
                                 row_number() over (partition by su.external_id, reg.register_id order by ri.meterreadingdatetime desc) as latest_read_per_register
                          from ref_cdb_supply_contracts su
                          inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'E'
                          inner join ref_meters mt on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and mt.removeddate is null
                          inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
                          inner join ref_readings_internal_valid ri on ri.account_id = su.external_id and ri.register_id = reg.register_id
                         ) reads
                    left outer join ref_calculated_eac pa_eac on pa_eac.account_id = reads.external_id and reads.register_id = pa_eac.register_id and reads.meterreadingdatetime = pa_eac.read_max_created_date_elec
                    left outer join ref_calculated_igl_ind_eac ig_eac on ig_eac.account_id = reads.external_id and reads.register_id = ig_eac.register_id and reads.meterreadingdatetime = ig_eac.read_max_datetime_elec
                    left outer join ref_estimates_elec_internal ee on ee.account_id = reads.external_id and ee.mpan = reads.meterpointnumber and ee.register_id = reads.registers_registerreference and ee.serial_number = reads.meterserialnumber and ee.effective_from = reads.meterreadingdatetime
                    left outer join ref_estimates_elec_internal prev_ee on prev_ee.account_id = reads.external_id and prev_ee.mpan = reads.meterpointnumber and prev_ee.register_id = reads.registers_registerreference and prev_ee.serial_number = reads.meterserialnumber and prev_ee.effective_from = reads.prev_reading_datetime
                    left outer join ref_cdb_registrations creg on creg.id = reads.registration_id
                    left outer join ref_cdb_quotes q on q.id = creg.quote_id
                    ) x

                    where x.account_id = 1831 and
                             x.latest_read_per_register = 1
                    group by x.account_id, x.latest_reading_datetime
                    order by x.account_id, x.latest_reading_datetime;

select
                        x.account_id as account_id,
                        x.latest_reading_datetime as reading_datetime,
                        max(x.pa_cons_gas_acc) as pa_cons_gas,
                        max(x.igl_ind_aq_acc) as igl_ind_aq,
                        max(x.ind_aq_acc) as ind_aq,
                        cast(max(x.quotes_aq_acc) as double precision) as quotes_aq,
                        getdate() as etlchange
                        from (
                        select
                        reads.external_id as account_id,
                        reads.register_id as register_id,
                        reads.register_reading_id reading_id,
                        reads.meterreadingdatetime as reading_datetime,
                        reads.latest_reading_datetime,
                        reads.latest_read_per_register,

                        pa_aq.igloo_aq as pa_cons_gas_reg,
                        ig_aq.igl_ind_aq as igl_ind_aq_reg,
                        reads.estimation_value as ind_aq_reg,
                        q.gas_usage quotes_aq,

                        sum(pa_aq.igloo_aq) over (partition by reads.external_id, reads.meterreadingdatetime) as pa_cons_gas_acc,
                        sum(ig_aq.igl_ind_aq) over (partition by reads.external_id, reads.meterreadingdatetime) as igl_ind_aq_acc,
                        sum(reads.estimation_value) over (partition by reads.external_id, reads.meterreadingdatetime) as ind_aq_acc,
                        max(case when q.gas_usage is null then
                            (q.gas_projected - ( 3.65 * q.gas_standing)) / (q.gas_unit / 100)
                                 else q.gas_usage end) over
                          (partition by reads.external_id, reads.meterreadingdatetime) as quotes_aq_acc

                              from (select su.external_id, su.registration_id,  mp.meterpointnumber, reg.register_id,  ri.register_reading_id, mt.meterserialnumber, reg.registers_eacaq, reg.registers_registerreference, ri.meterreadingdatetime,
                                           max(ri.meterreadingdatetime) over (partition by su.external_id) as latest_reading_datetime,
                                           row_number() over (partition by su.external_id, reg.register_id order by ri.meterreadingdatetime desc) as latest_read_per_register,
                                           (select top 1 estimation_value
                                             from ref_estimates_gas_internal eg
                                             where eg.account_id = ri.account_id
                                               and ri.meterpointnumber = eg.mprn
                                               and ri.registerreference = eg.register_id
                                               and ri.meterserialnumber = eg.serial_number
                                             order by eg.effective_from desc) as estimation_value
                                    from ref_cdb_supply_contracts su
                                    inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G'
                                    inner join ref_meters mt on mt.account_id = su.external_id and mt.meter_point_id = mp.meter_point_id and mt.removeddate is null
                                    inner join ref_registers reg on reg.account_id = su.external_id and reg.meter_id = mt.meter_id
                                    inner join ref_readings_internal_valid ri on ri.account_id = su.external_id and ri.register_id = reg.register_id
                                    ) reads
                        left outer join ref_calculated_aq pa_aq on pa_aq.account_id = reads.external_id and reads.register_id = pa_aq.register_id and reads.meterreadingdatetime = pa_aq.read_max_created_date_gas
                        left outer join ref_calculated_igl_ind_aq ig_aq on ig_aq.account_id = reads.external_id and reads.register_id = ig_aq.register_id and reads.meterreadingdatetime = ig_aq.read_max_datetime_gas
                        left outer join ref_cdb_registrations creg on creg.id = reads.registration_id
                        left outer join ref_cdb_quotes q on q.id = creg.quote_id
                        ) x

                        where
                                 x.latest_read_per_register = 1
                        group by x.account_id, x.latest_reading_datetime
                        order by x.account_id, x.latest_reading_datetime;

select * from ref_estimates_gas_internal where account_id in (1833);

select meterpointtype from ref_meterpoints group by meterpointtype;

select * from ref_calculated_tado_efficiency_batch
where mmh_tado_status='complete' and heating_source='oilboiler';

select * from ref_api_logs where api_key = 37198;

select account_id, meter_point_id from ref_meterpoints
where supplyenddate is null
group by account_id, meter_point_id
having count(*)>1;



select * from ref_meterpoints where account_id = 2831;
select * from ref_registers where account_id = 2831;

select segment, count(*) from ref_calculated_tado_efficiency_batch
group by segment;

select * from ref_calculated_tado_efficiency_batch t
-- inner join ref_meterpoints mp on mp.account_id = t.account_id
where segment = 10;

select * from ref_meterpoints where account_id=8360;