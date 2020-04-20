-- ELEC --

-- Payment adequacy on-demand update of annualised consumption
create or replace view vw_annualised_consumption_elec_on_demand as
select st.account_id,
       st.elec_GSP,
       st.elec_ssc,
       st.meterpoint_id,
       st.supplyend_date,
       st.meter_removed_date,
       st.register_id,
       st.no_of_digits,
       st.read_min_created_date_elec                                                  as read_min_created_date_elec,
       st.read_max_created_date_elec                                                  as read_max_created_date_elec,
       st.read_min_readings_elec                                                      as read_min_readings_elec,
       st.read_max_readings_elec                                                      as read_max_readings_elec,
       datediff(months, st.read_min_created_date_elec, st.read_max_created_date_elec) as read_months_diff_elec,
       datediff(days, st.read_min_created_date_elec, st.read_max_created_date_elec)   as read_days_diff_elec,
       st.read_consumption_elec,
       ppc,
       register_eac_elec                                                              as industry_eac,
       case
           when ppc != 0 then ((1 / ppc) * st.read_consumption_elec)
           else 0 end                                                                 as annualised_consumption
from (
         select mp_elec.account_id                   as account_id,
                mp_elec.meter_point_id               as meterpoint_id,
                mp_elec.supplyenddate                as supplyend_date,
                mtrs_elec.removeddate                as meter_removed_date,
                reg_elec.register_id                 as register_id,
                max(read_valid.no_of_digits)         as no_of_digits,
                rma_gsp.attributes_attributevalue    as elec_GSP,
                rma_ssc.attributes_attributevalue    as elec_ssc,
                max(reg_elec.registers_eacaq)        as register_eac_elec,
                min(read_valid.meterreadingdatetime) as read_min_created_date_elec,
                max(read_valid.meterreadingdatetime) as read_max_created_date_elec,
                min(read_valid.corrected_reading)    as read_min_readings_elec,
                max(read_valid.corrected_reading)    as read_max_readings_elec,
                max(read_valid.corrected_reading) -
                min(read_valid.corrected_reading)    as read_consumption_elec,
                (select sum(ppc_sum)
                 from ref_d18_igloo_ppc
                 where gsp_group_id = rma_gsp.attributes_attributevalue
                   and ss_conf_id = rma_ssc.attributes_attributevalue
                   and cast(time_pattern_regime as bigint) = reg_elec.registers_tpr
                   and pcl_id = cast(rma_pcl.attributes_attributevalue as integer)
                   and st_date between min(read_valid.meterreadingdatetime) and max(read_valid.meterreadingdatetime)
                 group by gsp_group_id, ss_conf_id)     ppc
         from ref_meterpoints mp_elec
                  inner join ref_meterpoints_attributes rma_gsp on mp_elec.account_id = rma_gsp.account_id and
                                                                   mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                                   rma_gsp.attributes_attributename = 'GSP'
                  inner join ref_meterpoints_attributes rma_ssc on mp_elec.account_id = rma_ssc.account_id and
                                                                   mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                                   rma_ssc.attributes_attributename = 'SSC'
                  inner join ref_meterpoints_attributes rma_pcl on mp_elec.account_id = rma_pcl.account_id and
                                                                   mp_elec.meter_point_id = rma_pcl.meter_point_id and
                                                                   rma_pcl.attributes_attributename = 'Profile Class'
                  inner join ref_meters mtrs_elec on mp_elec.account_id = mtrs_elec.account_id and
                                                     mtrs_elec.meter_point_id = mp_elec.meter_point_id and
                                                     mtrs_elec.removeddate is NULL
                  inner join ref_registers reg_elec
                             on mp_elec.account_id = reg_elec.account_id and mtrs_elec.meter_id = reg_elec.meter_id
                  left outer join vw_corrected_round_clock_reading_pa read_valid
                                  on mp_elec.account_id = read_valid.account_id and
                                     read_valid.register_id = reg_elec.register_id
         where mp_elec.meterpointtype = 'E'
           and (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
         group by mp_elec.account_id,
                  mp_elec.meter_point_id,
                  reg_elec.register_id,
                  reg_elec.registers_tpr,
                  mp_elec.supplyenddate,
                  mtrs_elec.removeddate,
                  rma_gsp.attributes_attributevalue,
                  rma_ssc.attributes_attributevalue,
                  rma_pcl.attributes_attributevalue
     ) st;
;
-- igl_ind_eac on demand update
create or replace view vw_igloo_ind_eac_on_demand as
    with cumulative_ppc as (
        select gsp_group_id,
               ss_conf_id,
               cast(time_pattern_regime as bigint)                                                                 as tpr,
               pcl_id,
               st_date,
               sum(ppc_sum)
               over (partition by gsp_group_id, ss_conf_id, tpr, pcl_id order by st_date rows unbounded preceding) as cumulative_ppc,
               count(ppc_sum)
               over (partition by gsp_group_id, ss_conf_id, tpr, pcl_id order by st_date rows unbounded preceding) as cumulative_ppc_count
        from (select * from ref_d18_igloo_ppc union select * from ref_d18_igloo_ppc_forecast) igl_ppc
    )
    select st.account_id,
           st.elec_GSP,
           st.elec_ssc,
           st.meterpoint_id,
           st.supplyend_date,
           st.meter_removed_date,
           st.register_id,
           st.no_of_digits,
           st.read_min_datetime_elec                                                         as read_min_datetime_elec,
           st.read_max_datetime_elec                                                         as read_max_datetime_elec,
           st.read_min_readings_elec                                                         as read_min_readings_elec,
           st.read_max_readings_elec                                                         as read_max_readings_elec,
           datediff(months, st.read_min_datetime_elec, st.read_max_datetime_elec)            as read_months_diff_elec,
           coalesce(datediff(days, st.read_min_datetime_elec, st.read_max_datetime_elec), 0) as read_days_diff_elec,
           0::bigint                                                                         as no_of_ppc_rows,
           0::bigint                                                                         as no_of_bpp_rows,
           st.read_consumption_elec,
           st.profile_class,
           st.tpr,
           cppc_latest.cumulative_ppc - cppc_previous.cumulative_ppc                         as ppc,
           0::double precision                                                              as bpp,
           st.smooth_param                                                                   as sp,
           st.total_reads                                                                    as total_reads,
           register_eac_elec                                                                 as industry_eac_register,
           st.previous_eac                                                                   as previous_ind_eac_estimates,
           st.latest_eac                                                                     as latest_ind_eac_estimates,
           round(calculate_igl_ind_eac(coalesce(st.smooth_param, 0), coalesce(ppc, 0),
                                       coalesce(st.read_consumption_elec, 0),
                                       coalesce(st.previous_eac, 0)), 1)                     as igl_ind_eac,
           getdate()                                                                         as etlchange
    from (select mp_elec.account_id                   as account_id,
                 mp_elec.meter_point_id               as meterpoint_id,
                 mp_elec.supplyenddate                as supplyend_date,
                 mtrs_elec.removeddate                as meter_removed_date,
                 reg_elec.register_id                 as register_id,
                 rma_gsp.attributes_attributevalue    as elec_GSP,
                 rma_ssc.attributes_attributevalue    as elec_ssc,
                 max(read_valid.no_of_digits)         as no_of_digits,
                 max(reg_elec.registers_eacaq)        as register_eac_elec,
                 min(read_valid.meterreadingdatetime) as read_min_datetime_elec,
                 max(read_valid.meterreadingdatetime) as read_max_datetime_elec,
                 min(read_valid.corrected_reading)    as read_min_readings_elec,
                 max(read_valid.corrected_reading)    as read_max_readings_elec,
                 coalesce(max(read_valid.corrected_reading) - min(read_valid.corrected_reading),
                          0)                          as read_consumption_elec,
                 rma_pcl.attributes_attributevalue    as profile_class,
                 reg_elec.registers_tpr               as tpr,
                 2                                    as smooth_param,
                 read_valid.total_reads               as total_reads,
                 coalesce(read_valid.previous_eac, 0) as previous_eac,
                 coalesce(read_valid.latest_eac, 0)   as latest_eac
          from ref_meterpoints mp_elec
                   inner join ref_meterpoints_attributes rma_gsp on mp_elec.account_id = rma_gsp.account_id and
                                                                    mp_elec.meter_point_id = rma_gsp.meter_point_id and
                                                                    rma_gsp.attributes_attributename = 'GSP'
                   inner join ref_meterpoints_attributes rma_ssc on mp_elec.account_id = rma_ssc.account_id and
                                                                    mp_elec.meter_point_id = rma_ssc.meter_point_id and
                                                                    rma_ssc.attributes_attributename = 'SSC'
                   inner join ref_meterpoints_attributes rma_pcl on mp_elec.account_id = rma_pcl.account_id and
                                                                    mp_elec.meter_point_id = rma_pcl.meter_point_id and
                                                                    rma_pcl.attributes_attributename = 'Profile Class'
                   inner join ref_meters mtrs_elec
                              on mtrs_elec.account_id = mp_elec.account_id and
                                 mtrs_elec.meter_point_id = mp_elec.meter_point_id and mtrs_elec.removeddate is NULL
                   inner join ref_registers reg_elec
                              on reg_elec.account_id = mp_elec.account_id and mtrs_elec.meter_id = reg_elec.meter_id
                   left outer join (select max(
                                           case when y.n = 1 then estimation_value else 0 end)
                                           over (partition by y.register_id) latest_eac,
                                           max(
                                           case when y.n = 2 then estimation_value else 0 end)
                                           over (partition by y.register_id) previous_eac,
                                           y.account_id,
                                           y.meterpointnumber,
                                           y.registerreference,
                                           y.register_id,
                                           y.no_of_digits,
                                           y.meterreadingdatetime,
                                           y.meterreadingcreateddate,
                                           y.corrected_reading,
                                           y.total_reads
                                    from (select r.*,
                                                 dense_rank()
                                                 over (partition by account_id, register_id order by meterreadingdatetime desc) n,
                                                 count(*) over (partition by account_id, register_id)                           total_reads
                                          from vw_corrected_round_clock_reading_pa r) y
                                             left outer join ref_estimates_elec_internal ee
                                                             on ee.account_id = y.account_id and
                                                                y.meterpointnumber = ee.mpan and
                                                                y.registerreference = ee.register_id
                                                                 and y.meterserialnumber = ee.serial_number and
                                                                ee.effective_from = y.meterreadingdatetime
                                    where y.n <= 2) read_valid
                                   on read_valid.account_id = mp_elec.account_id and
                                      read_valid.register_id = reg_elec.register_id

          where mp_elec.meterpointtype = 'E'
            and (mp_elec.supplyenddate is null or mp_elec.supplyenddate > getdate())
          group by mp_elec.account_id,
                   mp_elec.meter_point_id,
                   reg_elec.register_id,
                   reg_elec.registers_tpr,
                   mp_elec.supplyenddate,
                   mtrs_elec.removeddate,
                   rma_gsp.attributes_attributevalue,
                   rma_ssc.attributes_attributevalue,
                   rma_pcl.attributes_attributevalue,
                   read_valid.total_reads,
                   read_valid.previous_eac,
                   read_valid.latest_eac,
                   read_valid.register_id) st
             left join cumulative_ppc cppc_latest on cppc_latest.tpr = st.tpr and
                                                     cppc_latest.pcl_id = (st.profile_class::int) and
                                                     cppc_latest.ss_conf_id = st.elec_ssc and
                                                     cppc_latest.gsp_group_id = st.elec_GSP and
                                                     cppc_latest.st_date = trunc(st.read_max_datetime_elec)
             left join cumulative_ppc cppc_previous on cppc_previous.tpr = st.tpr and
                                                       cppc_previous.pcl_id = (st.profile_class::int) and
                                                       cppc_previous.ss_conf_id = st.elec_ssc and
                                                       cppc_previous.gsp_group_id = st.elec_GSP and
                                                       cppc_previous.st_date = trunc(st.read_min_datetime_elec)
;
-- view to link the on demand annualised consumption and igl_ind_eac views
drop view vw_cons_acc_elec_on_demand;
create or replace view vw_cons_acc_elec_on_demand as
select batch.account_id,
       nullif(demand.latest_elec_read_date, '1970-01-01')                          as reading_datetime,
       coalesce(ao.ann_cons_override, demand.annualised_consumption)               as pa_cons_elec,
       coalesce(ao.igl_ind_override, demand.igl_ind_eac)                           as igl_ind_eac,
       coalesce(ao.ind_override, batch.ind_eac)                                    as ind_eac,
       coalesce(ao.quote_override, batch.quotes_eac)                               as quotes_eac,
       get_best_consumption(coalesce(ao.igl_ind_override, demand.igl_ind_eac),
                            coalesce(ao.ind_override, batch.ind_eac),
                            coalesce(ao.ann_cons_override, demand.annualised_consumption),
                            coalesce(ao.quote_override, batch.quotes_eac), 'elec') as ca_source,
       case ca_source
           when 'pa_cons_elec' then coalesce(ao.ann_cons_override, demand.annualised_consumption)
           when 'igl_ind_eac' then coalesce(ao.igl_ind_override, demand.igl_ind_eac)
           when 'ind_eac' then coalesce(ao.ind_override, batch.ind_eac)
           when 'quotes_eac' then coalesce(ao.quote_override, batch.quotes_eac)
           end                                                                     as ca_value,
       demand.ac_read_days_diff_elec,
       batch.etlchange
from (select acc_id                                        as account_id,
             sum(annualised_consumption)                   as annualised_consumption,
             min(ac_read_days_diff_elec)                   as ac_read_days_diff_elec,
             min(nvl(latest_elec_read_date, '1970-01-01')) as latest_elec_read_date,
             sum(igl_ind_eac)                              as igl_ind_eac
      from (select coalesce(ac.account_id, iie.account_id) as acc_id,
                   coalesce(ac.annualised_consumption, 0)  as annualised_consumption,
                   coalesce(ac.read_days_diff_elec, 0)     as ac_read_days_diff_elec,
                   coalesce(ac.read_max_created_date_elec,
                            iie.read_max_datetime_elec)    as latest_elec_read_date,
                   coalesce(iie.igl_ind_eac, 0)            as igl_ind_eac
            from vw_annualised_consumption_elec_on_demand ac
                     full join vw_igloo_ind_eac_on_demand iie
                               on ac.account_id = iie.account_id and ac.register_id = iie.register_id) register_level
      group by acc_id) demand
         inner join ref_consumption_accuracy_elec batch on batch.account_id = demand.account_id
         left join vw_cons_acc_account_overrides ao on ao.account_id = demand.account_id and ao.meterpointtype = 'E'
;

-- GAS --

-- update to readings internal pa view
create or replace view vw_corrected_round_clock_reading_pa(account_id, meter_point_id, meter_id, meter_reading_id,
                                                           register_id,
                                                           register_reading_id, billable, haslivecharge,
                                                           hasregisteradvance,
                                                           meterpointnumber, meterpointtype, meterreadingcreateddate,
                                                           meterreadingdatetime, meterreadingsourceuid,
                                                           meterreadingstatusuid,
                                                           meterreadingtypeuid, meterserialnumber, registerreference,
                                                           required,
                                                           no_of_digits, readingvalue, previous_reading,
                                                           current_reading,
                                                           max_previous_reading, max_reading, corrected_reading,
                                                           meter_rolled_over,
                                                           etlchange) as
SELECT s.account_id,
       s.meter_point_id,
       s.meter_id,
       s.meter_reading_id,
       s.register_id,
       s.register_reading_id,
       s.billable,
       s.haslivecharge,
       s.hasregisteradvance,
       s.meterpointnumber,
       s.meterpointtype,
       s.meterreadingcreateddate,
       s.meterreadingdatetime,
       s.meterreadingsourceuid,
       s.meterreadingstatusuid,
       s.meterreadingtypeuid,
       s.meterserialnumber,
       s.registerreference,
       s.required,
       s.no_of_digits,
       s.readingvalue,
       s.previous_reading,
       s.current_reading,
       s.max_previous_reading,
       s.max_reading,
       round_the_clock_reading_check_digits_v1(s.current_reading,
                                               s.previous_reading,
                                               s.max_reading,
                                               CASE
                                                   WHEN ((s.max_previous_reading <> s.current_reading) AND
                                                         (s.max_previous_reading > (s.max_reading - (10000)::double precision)))
                                                       THEN 'Y'::character varying
                                                   ELSE 'N'::character varying END) AS corrected_reading,
       CASE
           WHEN ((s.max_previous_reading <> s.current_reading) AND
                 (s.max_previous_reading > (s.max_reading - (10000)::double precision))) THEN 'Y'::character varying
           ELSE 'N'::character varying END                                          AS meter_rolled_over,
       ('now'::character varying)::timestamp with time zone                         AS etlchange
FROM (SELECT ri.account_id,
             ri.meter_point_id,
             ri.meter_id,
             ri.meter_reading_id,
             ri.register_id,
             ri.register_reading_id,
             ri.billable,
             ri.haslivecharge,
             ri.hasregisteradvance,
             ri.meterpointnumber,
             ri.meterpointtype,
             ri.meterreadingcreateddate,
             ri.meterreadingdatetime,
             ri.meterreadingsourceuid,
             ri.meterreadingstatusuid,
             ri.meterreadingtypeuid,
             ri.meterserialnumber,
             ri.readingvalue,
             ri.registerreference,
             ri.required,
             COALESCE((rega.registersattributes_attributevalue)::integer, 0)                                                                           AS no_of_digits,
             COALESCE(pg_catalog.lead(ri.readingvalue, 1)
                      OVER ( PARTITION BY ri.account_id, ri.register_id ORDER BY ri.meterreadingdatetime DESC),
                      (0)::double precision)                                                                                                           AS previous_reading,
             COALESCE(ri.readingvalue, (0)::double precision)                                                                                          AS current_reading,
             (power((10)::double precision,
                    (COALESCE((rega.registersattributes_attributevalue)::integer, 0))::double precision) -
              (1)::double precision)                                                                                                                   AS max_reading,
             "max"(ri.readingvalue)
             OVER ( PARTITION BY ri.account_id, ri.register_id ORDER BY ri.meterreadingdatetime DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS max_previous_reading
      FROM (ref_readings_internal_pa ri
               LEFT JOIN ref_registers_attributes rega
                         ON ((((rega.register_id = ri.register_id) AND (rega.account_id = ri.account_id)) AND
                              ((rega.registersattributes_attributename)::text =
                               ('No_Of_Digits'::character varying)::text))))
      WHERE (((((((ri.meterreadingsourceuid)::text = ('CUSTOMER'::character varying)::text) AND
                 ((ri.meterreadingstatusuid)::text = ('VALID'::character varying)::text)) AND
                ((ri.meterreadingtypeuid)::text = ('ACTUAL'::character varying)::text)) AND (ri.billable = true)) OR
              (((((ri.meterreadingsourceuid)::text = ('DC'::character varying)::text) AND
                 ((ri.meterreadingstatusuid)::text = ('VALID'::character varying)::text)) AND
                ((ri.meterreadingtypeuid)::text = ('ACTUAL'::character varying)::text)) AND (ri.billable = true))) OR
             (((((ri.meterreadingsourceuid)::text = ('DCOPENING'::character varying)::text) AND
                ((ri.meterreadingstatusuid)::text = ('VALID'::character varying)::text)) AND
               ((ri.meterreadingtypeuid)::text = ('ACTUAL'::character varying)::text)) AND (ri.billable = true)))
      ORDER BY ri.account_id, ri.register_id, ri.meterreadingdatetime) s;
alter table vw_corrected_round_clock_reading_pa
    owner to igloo;

-- view for readings from ensek and nrl
create or replace view vw_readings_aq_all_on_demand as
select account_id,
       register_id,
       meterpointnumber,
       meter_point_id,
       meter_id,
       readingvalue,
       meterreadingdatetime,
       meterreadingsourceuid,
       meter_reading_id,
       meterpointtype,
       etlchange
from (
         select *,
                -- this rank will be 1 for any unique values and where a duplicate has occurred the values shall be taken
                -- from a table in order of ensek (ref_readings_internal_valid), nosi, nrl; whichever is present
                -- it will not distinguish between duplicates coming from the same table
                row_number() over (partition by account_id, register_id, meterreadingdatetime
                    order by from_table) as uniqueness_rank

         from (
                  select account_id,
                         register_id,
                         meterpointnumber,
                         meter_point_id,
                         meter_id,
                         readingvalue,
                         meterreadingdatetime,
                         meterreadingsourceuid,
                         'ensek' as from_table,
                         meter_reading_id,
                         meterpointtype,
                         etlchange
                  from vw_corrected_round_clock_reading_pa

                  union

                  select distinct nrl.account_id,
                                  register_id,
                                  meterpointnumber,
                                  meter_point_id,
                                  meter_id,
                                  readingvalue,
                                  meterreadingdatetime,
                                  meterreadingsourceuid,
                                  'nrl' as from_table,
                                  meter_reading_id,
                                  meterpointtype,
                                  etlchange
                  from ref_readings_internal_nrl nrl
                           inner join (select account_id, nrl_date, nrl_value, max(apd) as max_apd
                                       from (
                                                select nrl.*,
                                                       nrl.readingvalue                                                    as nrl_value,
                                                       nrl.meterreadingdatetime                                            as nrl_date,
                                                       rriv.readingvalue                                                   as rriv_value,
                                                       rriv_value - nrl_value + case
                                                                                    when rriv_value < nrl_value
                                                                                        then pow(10, coalesce(
                                                                                            nullif(rriv.no_of_digits, 0),
                                                                                            greatest(len(nrl_value), 5)))
                                                                                    else 0 end                             as advance,
                                                       datediff(days, nrl.meterreadingdatetime, rriv.meterreadingdatetime) as days_diff,
                                                       advance / days_diff                                                 as apd
                                                from ref_readings_internal_nrl nrl
                                                         inner join ref_readings_internal_valid rriv
                                                                    on rriv.meterreadingdatetime > nrl.meterreadingdatetime
                                                                        and rriv.account_id = nrl.account_id
                                                                        and
                                                                       rriv.meterserialnumber = nrl.meterserialnumber
                                                                        and rriv.meterpointtype = 'G'
                                                where days_diff >= 14
                                            ) nrl_stats
                                       group by account_id, nrl_date, nrl_value
                                       having max_apd < 50) valid_nrl
                                      on nrl.account_id = valid_nrl.account_id and
                                         nrl.meterreadingdatetime = valid_nrl.nrl_date and
                                         nrl.readingvalue = valid_nrl.nrl_value
                  where readingvalue notnull
              ) readings_all

         where readings_all.readingvalue notnull
           and meterpointtype = 'G'
     ) ranked
where uniqueness_rank = 1
order by account_id, register_id, meterreadingdatetime
;

-- Payment adequacy on-demand update of annualised consumption
create or replace view vw_annualised_consumption_gas_on_demand as
select st.account_id,
       st.gas_LDZ,
       st.gas_Imperial_Meter_Indicator,
       st.meterpoint_id,
       st.supplyend_date,
       st.meter_removed_date,
       st.register_id,
       st.no_of_digits,
       st.read_min_created_date_gas                               as read_min_created_date_gas,
       st.read_max_created_date_gas                               as read_max_created_date_gas,
       st.read_min_readings_gas                                   as read_min_readings_gas,
       st.read_max_readings_gas                                   as read_max_readings_gas,
       datediff(months, st.read_min_created_date_gas,
                st.read_max_created_date_gas)                     as read_months_diff_gas,
       datediff(days, st.read_min_created_date_gas,
                st.read_max_created_date_gas)                     as read_days_diff_gas,
       st.read_consumption_gas,
       st.cv,
       waalp,
       ((st.read_consumption_gas * 1.02264 * st.cv * st.U) / 3.6) as rmq,
       st.register_eac_gas                                        as industry_aq,
       case
           when waalp != 0 then (st.read_consumption_gas * 1.02264 * st.cv * st.U / 3.6) * 365 /
                                waalp
           else 0 end                                             as annualised_consumption
from (select mp_gas.account_id                                                                                                             as account_id,
             mp_gas.meter_point_id                                                                                                         as meterpoint_id,
             mp_gas.supplyenddate                                                                                                          as supplyend_date,
             mtrs_gas.removeddate                                                                                                          as meter_removed_date,
             reg_gas.register_id                                                                                                           as register_id,
             max(read_valid.no_of_digits)                                                                                                  as no_of_digits,
             rma_ldz.attributes_attributevalue                                                                                             as gas_LDZ,
             rma_imp.attributes_attributevalue                                                                                             as gas_Imperial_Meter_Indicator,
             max(reg_gas.registers_eacaq)                                                                                                  as register_eac_gas,
             min(trunc(read_valid.meterreadingdatetime))                                                                                   as read_min_created_date_gas,
             max(trunc(read_valid.meterreadingdatetime))                                                                                   as read_max_created_date_gas,
             min(read_valid.corrected_reading)                                                                                             as read_min_readings_gas,
             max(read_valid.corrected_reading)                                                                                             as read_max_readings_gas,
             max(read_valid.corrected_reading) -
             min(read_valid.corrected_reading)                                                                                             as read_consumption_gas,
             (select sum((1 + (0.5 * waalp.value * waalp.variance)) * waalp.forecastdocumentation)
              from ref_alp_igloo_daf_wcf waalp
              where waalp.ldz = trim(rma_ldz.attributes_attributevalue)
                and waalp.date between min(trunc(read_valid.meterreadingdatetime)) and max(trunc(read_valid.meterreadingdatetime)))        as waalp,
             (select 0.5 * avg(cv.value)
              from ref_alp_igloo_cv cv
              where cv.ldz = trim(rma_ldz.attributes_attributevalue)
                and cv.applicable_for between min(trunc(read_valid.meterreadingdatetime)) and max(trunc(read_valid.meterreadingdatetime))) as cv,
             case
                 when rma_imp.attributes_attributevalue in ('N') then 1.00
                 else 2.83 end                                                                                                             as U
      from ref_meterpoints mp_gas
               inner join ref_meterpoints_attributes rma_ldz on mp_gas.account_id = rma_ldz.account_id and
                                                                mp_gas.meter_point_id = rma_ldz.meter_point_id and
                                                                rma_ldz.attributes_attributename = 'LDZ'
               inner join ref_meterpoints_attributes rma_imp on mp_gas.account_id = rma_imp.account_id and
                                                                mp_gas.meter_point_id = rma_imp.meter_point_id and
                                                                rma_imp.attributes_attributename = 'Gas_Imperial_Meter_Indicator'
               inner join ref_meters mtrs_gas on mp_gas.account_id = mtrs_gas.account_id and
                                                 mtrs_gas.meter_point_id = mp_gas.meter_point_id and
                                                 mtrs_gas.removeddate is NULL
               inner join ref_registers reg_gas
                          on mp_gas.account_id = reg_gas.account_id and mtrs_gas.meter_id = reg_gas.meter_id
               inner join vw_corrected_round_clock_reading_pa read_valid
                          on mp_gas.account_id = read_valid.account_id and
                             read_valid.register_id = reg_gas.register_id and
                             read_valid.meterreadingdatetime > '2014-10-01'
      where mp_gas.meterpointtype = 'G'
        and (mp_gas.supplyenddate is null or mp_gas.supplyenddate
          > getdate())
      group by mp_gas.account_id,
               mp_gas.meter_point_id,
               reg_gas.register_id,
               mp_gas.supplyenddate,
               mtrs_gas.removeddate,
               rma_ldz.attributes_attributevalue,
               rma_imp.attributes_attributevalue) st
;
-- igl_ind_eac on demand update
create or replace view vw_igloo_ind_aq_on_demand as
select account_id,
       LDZ                                                             as gas_ldz,
       gas_imperial_meter_indicator,
       meter_point_id                                                  as meterpoint_id,
       supplyenddate                                                   as supplyend_date,
       removeddate                                                     as meter_removed_date,
       register_id,
       no_of_digits,
       open_date                                                       as read_min_datetime_gas,
       close_date                                                      as read_max_datetime_gas,
       open_val                                                        as read_min_readings_gas,
       close_val                                                       as read_max_readings_gas,
       datediff(months, open_date, close_date)                         as read_months_diff_gas,
       datediff(days, open_date, close_date)                           as read_days_diff_gas,
       meter_advance                                                   as read_consumption_gas,
       avg_cv                                                          as cv,
       cwaalp                                                          as waalp,
       meter_advance * 1.02264 * avg_cv * U * (1 / 3.6)                as rmq,
       registers_eacaq                                                 as industry_aq_on_register,
       (select top 1 estimation_value
        from ref_estimates_gas_internal regi
        where calc_params.account_id = regi.account_id
          and calc_params.meterpointnumber = regi.mprn
        order by effective_from desc)                                  as industry_aq_on_estimates,
       U                                                               as u,
       meter_advance * 1.02264 * avg_cv * U * (1 / 3.6) * 365 / cwaalp as igl_ind_aq,
       getdate()                                                       as etlchange
from (
         select distinct read_pairs.account_id,
                         read_pairs.register_id,
                         rma_ldz.attributes_attributevalue                                      as LDZ,
                         case when rma_imp.attributes_attributevalue = 'Y' then 2.83 else 1 end as U,
                         read_pairs.open_date,
                         read_pairs.open_val,
                         read_pairs.close_date,
                         read_pairs.close_val,
                         case
                             when read_pairs.close_val < read_pairs.open_val --meter has rolled over
                                 then read_pairs.close_val - read_pairs.open_val + power(10, len(read_pairs.open_val))
                             else read_pairs.close_val - read_pairs.open_val
                             end                                                                as meter_advance,
                         (select sum((1 + (coalesce(waalp.value * 0.5, 0) * (waalp.variance))) *
                                     (waalp.forecastdocumentation)) --TODO: if/when the weather data is corrected, remove the /2
                          from ref_alp_igloo_daf_wcf waalp
                          where waalp.ldz = trim(rma_ldz.attributes_attributevalue)
                            and waalp.date >= read_pairs.open_date
                            and waalp.date < read_pairs.close_date)                             as cwaalp,
                         (select avg(cv.value / 2) --TODO: if/when the CV data is corrected, remove the /2
                          from ref_alp_igloo_cv cv
                          where cv.ldz = trim(rma_ldz.attributes_attributevalue)
                            and cv.applicable_for >= read_pairs.open_date
                            and cv.applicable_for < read_pairs.close_date)                      as avg_cv,

                         -- additional info for the output table
                         rma_imp.attributes_attributevalue                                      as gas_imperial_meter_indicator,
                         reg_gas.meter_point_id,
                         reg_gas.meter_id,
                         reg_gas.registers_eacaq,
                         read_pairs.no_of_digits,
                         read_pairs.meterpointnumber,
                         rmp.supplyenddate,
                         rm.removeddate
         from
             -- open / close read pairs
             (select *
              from (select
                        --readings info
                        read_close.account_id,
                        read_close.register_id,
                        read_close.meterreadingdatetime                                                                    as close_date,
                        read_close.readingvalue                                                                            as close_val,
                        read_open.meterreadingdatetime                                                                     as open_date,
                        read_open.readingvalue                                                                             as open_val,
                        read_close.no_of_digits, -- just for output table
                        read_close.meterpointnumber,
                        --info to inform selection of valid pairs (rows that contain the best opening reading for each closing reading)
                        open_read_suitability_score(
                                datediff(days, read_open.meterreadingdatetime, read_close.meterreadingdatetime),
                                2)                                                                                         as orss,
                        min(open_read_suitability_score(
                                datediff(days, read_open.meterreadingdatetime, read_close.meterreadingdatetime), 2))
                        over (partition by read_close.account_id, read_close.register_id, read_close.meterreadingdatetime) as best_orss
                    from (select *
                          from (select rriv.*,
                                       row_number()
                                       over (partition by rriv.account_id, rriv.register_id order by rriv.meterreadingdatetime desc) as r
                                from vw_corrected_round_clock_reading_pa rriv
                                where rriv.meterpointtype = 'G'
                               ) ranked
                          where r = 1) read_close
                             inner join vw_readings_aq_all_on_demand read_open
                                        on read_open.register_id = read_close.register_id
                                            and read_open.meterreadingsourceuid not in ('DCOPENING', 'DC')
                                            --The following line can be removed to allow matching of register reads prior to account creation (NRL/NOSI)
                                            --and read_open.account_id = read_close.account_id
                                            and datediff(days, read_open.meterreadingdatetime,
                                                         read_close.meterreadingdatetime) between 273 and (365 * 3)
                   ) possible_read_pairs
              where orss = best_orss
                and open_date >= '2014-10-01' --the oldest date we have WAALP data for
             ) read_pairs
                 --get meter_point_id from ref_registers
                 left join ref_registers reg_gas
                           on read_pairs.account_id = reg_gas.account_id and
                              read_pairs.register_id = reg_gas.register_id --LDZ for the meterpoint
                 left join ref_meterpoints_attributes rma_ldz
                           on reg_gas.account_id = rma_ldz.account_id and
                              reg_gas.meter_point_id = rma_ldz.meter_point_id and
                              rma_ldz.attributes_attributename = 'LDZ' --Whether the meterpoint is Imperial or Metric
                 left join ref_meterpoints_attributes rma_imp
                           on reg_gas.account_id = rma_imp.account_id and
                              reg_gas.meter_point_id = rma_imp.meter_point_id and
                              rma_imp.attributes_attributename = 'Gas_Imperial_Meter_Indicator'
                 left join ref_meterpoints rmp
                           on read_pairs.account_id = rmp.account_id and
                              reg_gas.meter_point_id = rmp.meter_point_id
                 left join ref_meters rm
                           on rm.account_id = read_pairs.account_id and
                              rm.meter_point_id = reg_gas.meter_point_id and
                              rm.meter_id = reg_gas.meter_id
     ) calc_params
;

-- view to link the on demand annualised consumption and igl_ind_aq views
drop view vw_cons_acc_gas_on_demand;
create or replace view vw_cons_acc_gas_on_demand as
select batch.account_id,
       nullif(demand.latest_gas_read_date, '1970-01-01')                         as reading_datetime,
       coalesce(ao.ann_cons_override, demand.annualised_consumption)             as pa_cons_gas,
       coalesce(ao.igl_ind_override, demand.igl_ind_aq)                          as igl_ind_aq,
       coalesce(ao.ind_override, batch.ind_aq)                                   as ind_aq,
       coalesce(ao.quote_override, batch.quotes_aq)                              as quotes_aq,
       get_best_consumption(coalesce(ao.igl_ind_override, demand.igl_ind_aq),
                            coalesce(ao.ind_override, batch.ind_aq),
                            coalesce(ao.ann_cons_override, demand.annualised_consumption),
                            coalesce(ao.quote_override, batch.quotes_aq), 'gas') as ca_source,
       case ca_source
           when 'pa_cons_gas' then coalesce(ao.ann_cons_override, demand.annualised_consumption)
           when 'igl_ind_aq' then coalesce(ao.igl_ind_override, demand.igl_ind_aq)
           when 'ind_aq' then coalesce(ao.ind_override, batch.ind_aq)
           when 'quotes_aq' then coalesce(ao.quote_override, batch.quotes_aq)
           end                                                                   as ca_value,
       demand.ac_read_days_diff_gas,
       batch.etlchange
from (select acc_id                                       as account_id,
             sum(annualised_consumption)                  as annualised_consumption,
             min(ac_read_days_diff_gas)                   as ac_read_days_diff_gas,
             min(nvl(latest_gas_read_date, '1970-01-01')) as latest_gas_read_date,
             sum(igl_ind_aq)                              as igl_ind_aq
      from (select coalesce(ac.account_id, iie.account_id) as acc_id,
                   coalesce(ac.annualised_consumption, 0)  as annualised_consumption,
                   coalesce(ac.read_days_diff_gas, 0)      as ac_read_days_diff_gas,
                   coalesce(ac.read_max_created_date_gas,
                            iie.read_max_datetime_gas)     as latest_gas_read_date,
                   coalesce(iie.igl_ind_aq, 0)             as igl_ind_aq
            from vw_annualised_consumption_gas_on_demand ac
                     full join vw_igloo_ind_aq_on_demand iie
                               on ac.account_id = iie.account_id and ac.register_id = iie.register_id) register_level
      group by acc_id) demand
         inner join ref_consumption_accuracy_gas batch on batch.account_id = demand.account_id
         left join vw_cons_acc_account_overrides ao on ao.account_id = demand.account_id and ao.meterpointtype = 'G'
;