-- 1
create table ref_consumption_accuracy_override_meterpoints
(
    meterpoint_id    integer,
    override_type_id integer,
    effective_from   timestamp,
    effective_to     timestamp,
    notes            varchar(500)
);

alter table ref_consumption_accuracy_override_meterpoints
    owner to igloo;

-- 2
create table ref_consumption_accuracy_override_types
(
    id                integer distkey,
    name              varchar(20),
    description       varchar(500),
    igl_ind_override  integer,
    ind_override      integer,
    ann_cons_override integer,
    quote_override    integer,
    effective_from    timestamp default getdate(),
    effective_to      timestamp,
    jira              varchar(20)
)
    diststyle key
    sortkey (effective_to, effective_from, id);

alter table ref_consumption_accuracy_override_types
    owner to igloo;

-- 3
create or replace view vw_cons_acc_account_overrides as
select rm.account_id,
       rm.meterpointtype,
       max(ot.igl_ind_override)  as igl_ind_override,
       max(ot.ind_override)      as ind_override,
       max(ot.ann_cons_override) as ann_cons_override,
       max(ot.quote_override)    as quote_override
from ref_meterpoints rm
         left join ref_consumption_accuracy_override_meterpoints om
                   on rm.meter_point_id = om.meterpoint_id and om.effective_from <= getdate() and
                      (nvl(om.effective_to, getdate() + 1) > getdate())
         left join ref_consumption_accuracy_override_types ot
                   on om.override_type_id = ot.id and ot.effective_from <= getdate() and
                      (nvl(ot.effective_to, getdate() + 1) > getdate())
group by rm.account_id, rm.meterpointtype

-- 4
create or replace view vw_cons_acc_elec as
select ca.account_id,
       ca.reading_datetime,
       coalesce(ao.ann_cons_override, ca.pa_cons_elec)                          as pa_cons_elec,
       coalesce(ao.igl_ind_override, ca.igl_ind_eac)                            as igl_ind_eac,
       coalesce(ao.ind_override, ca.ind_eac)                                    as ind_eac,
       coalesce(ao.quote_override, ca.quotes_eac)                               as quotes_eac,
       get_best_consumption(coalesce(ao.igl_ind_override, ca.igl_ind_eac),
                            coalesce(ao.ind_override, ca.ind_eac),
                            coalesce(ao.ann_cons_override, ca.pa_cons_elec),
                            coalesce(ao.quote_override, ca.quotes_eac), 'elec') as ca_source,
       case ca_source
           when 'pa_cons_elec' then coalesce(ao.ann_cons_override, ca.pa_cons_elec)
           when 'igl_ind_eac' then coalesce(ao.igl_ind_override, ca.igl_ind_eac)
           when 'ind_eac' then coalesce(ao.ind_override, ca.ind_eac)
           when 'quotes_eac' then coalesce(ao.quote_override, ca.quotes_eac)
           end                                                                  as ca_value,
       ca.etlchange
from ref_consumption_accuracy_elec ca
         left join vw_cons_acc_account_overrides ao
                   on ca.account_id = ao.account_id and ao.meterpointtype = 'E';

-- 5
create or replace view vw_cons_acc_gas as
select ca.account_id,
       ca.reading_datetime,
       coalesce(ao.ann_cons_override, ca.pa_cons_gas)                         as pa_cons_gas,
       coalesce(ao.igl_ind_override, ca.igl_ind_aq)                           as igl_ind_aq,
       coalesce(ao.ind_override, ca.ind_aq)                                   as ind_aq,
       coalesce(ao.quote_override, ca.quotes_aq)                              as quotes_aq,
       get_best_consumption(coalesce(ao.igl_ind_override, ca.igl_ind_aq),
                            coalesce(ao.ind_override, ca.ind_aq),
                            coalesce(ao.ann_cons_override, ca.pa_cons_gas),
                            coalesce(ao.quote_override, ca.quotes_aq), 'gas') as ca_source,
       case ca_source
           when 'pa_cons_gas' then coalesce(ao.ann_cons_override, ca.pa_cons_gas)
           when 'igl_ind_aq' then coalesce(ao.igl_ind_override, ca.igl_ind_aq)
           when 'ind_aq' then coalesce(ao.ind_override, ca.ind_aq)
           when 'quotes_aq' then coalesce(ao.quote_override, ca.quotes_aq)
           end                                                                as ca_value,
       ca.etlchange
from ref_consumption_accuracy_gas ca
         left join vw_cons_acc_account_overrides ao
                   on ca.account_id = ao.account_id and ao.meterpointtype = 'G';

-- 6
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

-- 7
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

-- 8
-- update to readings internal pa view
create or replace view vw_corrected_round_clock_reading_pa(account_id, meter_point_id, meter_id, meter_reading_id, register_id,
                                                register_reading_id, billable, haslivecharge, hasregisteradvance,
                                                meterpointnumber, meterpointtype, meterreadingcreateddate,
                                                meterreadingdatetime, meterreadingsourceuid, meterreadingstatusuid,
                                                meterreadingtypeuid, meterserialnumber, registerreference, required,
                                                no_of_digits, readingvalue, previous_reading, current_reading,
                                                max_previous_reading, max_reading, corrected_reading, meter_rolled_over,
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

-- 9
-- Fill override types
INSERT INTO public.ref_consumption_accuracy_override_types (id, name, description, igl_ind_override, ind_override,
                                                            ann_cons_override, quote_override, effective_from,
                                                            effective_to, jira)
VALUES (2, 'ind override', 'generic override for individual cases of ind errors', null, 0, null, null,
        '2019-11-14 12:15:10.102000', null, null);
INSERT INTO public.ref_consumption_accuracy_override_types (id, name, description, igl_ind_override, ind_override,
                                                            ann_cons_override, quote_override, effective_from,
                                                            effective_to, jira)
VALUES (4, 'quote_override', 'generic override for individual cases of quote errors', null, null, null, 0,
        '2019-11-14 12:15:10.102000', null, null);
INSERT INTO public.ref_consumption_accuracy_override_types (id, name, description, igl_ind_override, ind_override,
                                                            ann_cons_override, quote_override, effective_from,
                                                            effective_to, jira)
VALUES (1, 'igl_ind override', 'generic override for individual cases of igl_ind errors', 0, null, null, null,
        '2019-11-14 12:15:10.102000', null, null);
INSERT INTO public.ref_consumption_accuracy_override_types (id, name, description, igl_ind_override, ind_override,
                                                            ann_cons_override, quote_override, effective_from,
                                                            effective_to, jira)
VALUES (3, 'ann_cons override', 'generic override for individual cases of ann_cons errors', null, null, 0, null,
        '2019-11-14 12:15:10.102000', null, null);

-- Fill test case in override meterpoints (Ben's elec meterpoint)
INSERT INTO public.ref_consumption_accuracy_override_meterpoints (meterpoint_id, override_type_id, effective_from, effective_to, notes)
VALUES (86815, 4, '1970-01-01 00:00:00.001000', null, null);