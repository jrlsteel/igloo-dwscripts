--insert into ref_estimated_advance_gas
select x1.account_id,
       x1.register_id,
       x1.last_reading_date,
       x1.last_reading_value,
       x1.aq                                                        as igl_aq,
       x1.ind_aq,
       x1.effective_for,
       x1.igl_estimated_advance,
       x1.ind_estimated_advance,
       coalesce(rst_inner.tol_min * igl_estimated_advance / 100, 0) as igl_lower_threshold,
       coalesce(rst_inner.tol_max * igl_estimated_advance / 100, 0) as igl_higher_threshold,
       coalesce(rst_inner.tol_min * ind_estimated_advance / 100, 0) as ind_lower_threshold,
       coalesce(rst_inner.tol_max * ind_estimated_advance / 100, 0) as ind_inner_threshold,
       coalesce(rst_outer.tol_max * ind_estimated_advance / 100, 0) as ind_higher_threshold,
       coalesce(rra.registersattributes_attributevalue::int, 5)     as register_num_digits,
       getdate()                                                    as etlchange
from (select account_id,
             register_id,
             last_reading_date,
             last_reading_value,
             days_since_last_read,
             round(coalesce(aq, 0), 0)                                        as aq,
             round(coalesce(igl_ind_aq, 0), 0)                                as igl_ind_aq,
             round(coalesce(ind_aq, 0), 0)                                    as ind_aq,
             getdate()                                                        as effective_for,
             convert_kwh_to_cubic((aq * cwaalp) / 365, avg_cv, imp_indicator) as igl_estimated_advance,
             convert_kwh_to_cubic((ind_aq * (cast(days_since_last_read as double precision) / 365)), avg_cv,
                                  imp_indicator)                              as ind_estimated_advance
      from (select su.external_id                                               as account_id,
                   r.register_id,
                   reads.meterreadingdatetime                                   as last_reading_date,
                   reads.readingvalue                                           as last_reading_value,
                   datediff(days, reads.meterreadingdatetime, trunc(getdate())) as days_since_last_read,
                   greatest(mp.supplystartdate, associationstartdate)           as startdate,
                   least(mp.supplyenddate, associationenddate)                  as enddate,
                   coalesce(aq.igloo_aq, 0)                                     as igl_ind_aq,
                   coalesce(r.registers_eacaq, 0)                               as ind_aq,
                   case
                       when coalesce(aq.igloo_aq, 0) != 0 then aq.igloo_aq
                       else case
                                when coalesce(r.registers_eacaq, 0) != 0 then r.registers_eacaq
                                else 0 end end                                  as aq,
                   rma_imp.attributes_attributevalue                            as imp_indicator,
                   coalesce((select sum((1 + ((waalp.value / 2) * (waalp.variance))) *
                                        (waalp.forecastdocumentation)) --TODO: if/when the weather data is corrected, remove the /2
                             from ref_alp_igloo_daf_wcf waalp
                             where waalp.ldz = trim(rma.attributes_attributevalue)
                               and waalp.applicable_for >= reads.meterreadingdatetime
                               and waalp.applicable_for < current_date), 0)     as cwaalp,
                   coalesce((select avg(cv.value / 2)
                             from ref_alp_igloo_cv cv
                             where cv.ldz = trim(rma.attributes_attributevalue)
                               and cv.applicable_for >= reads.meterreadingdatetime
                               and cv.applicable_for < current_date), 0)        as avg_cv
            from ref_cdb_supply_contracts su
                     inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G' and
                                                      (least(mp.supplyenddate, mp.associationenddate) is null or
                                                       least(mp.supplyenddate, mp.associationenddate) >= current_date)
                     left outer join ref_meterpoints_attributes rma
                                     on rma.account_id = su.external_id and rma.meter_point_id = mp.meter_point_id and
                                        rma.attributes_attributename = 'LDZ'
                     left outer join ref_meterpoints_attributes rma_imp
                                     on rma_imp.account_id = su.external_id and
                                        rma_imp.meter_point_id = mp.meter_point_id and
                                        rma_imp.attributes_attributename = 'Gas_Imperial_Meter_Indicator'
                     inner join ref_meters m
                                on m.account_id = su.external_id and m.meter_point_id = mp.meter_point_id and
                                   m.removeddate is null
                     inner join ref_registers r on r.account_id = su.external_id and r.meter_id = m.meter_id
                     inner join (select *
                                 from (select account_id,
                                              register_id,
                                              meterreadingdatetime,
                                              readingvalue,
                                              row_number()
                                              over (partition by ri.account_id, register_id order by meterreadingdatetime desc) rownum
                                       from ref_readings_internal_valid ri
                                       order by ri.meterreadingdatetime desc) r1
                                 where r1.rownum = 1) reads
                                on reads.account_id = su.external_id and reads.register_id = r.register_id
                     left outer join ref_calculated_aq aq
                                     on aq.account_id = su.external_id and aq.register_id = r.register_id) x
      order by x.account_id, register_id) x1
         left join ref_stg_tolerances rst_inner
                   on
                           rst_inner.tol_group = 'industry_tolerance_gas'
                           and rst_inner.group_id = 3
                           and rst_inner.lookup_key = 'ind_gas_inner'
                           and (rst_inner.effective_to is null or rst_inner.effective_to >= getdate())
                           and rst_inner.effective_from <= getdate()
                           and x1.ind_aq >= rst_inner.lookup_range_min
                           and x1.ind_aq <= rst_inner.lookup_range_max
         left join ref_stg_tolerances rst_outer
                   on rst_outer.tol_group = 'industry_tolerance_gas'
                       and rst_outer.group_id = 3
                       and rst_outer.lookup_key = 'ind_gas_outer'
                       and (rst_outer.effective_to is null or rst_outer.effective_to >= getdate())
                       and rst_outer.effective_from <= getdate()
                       and x1.ind_aq >= rst_outer.lookup_range_min
                       and x1.ind_aq <= rst_outer.lookup_range_max
         left join ref_registers_attributes rra
                   on rra.account_id = x1.account_id and rra.register_id = x1.register_id and
                      registersattributes_attributename = 'No_Of_Digits'
order by x1.account_id, x1.register_id;


select count(*)
from ref_cdb_supply_contracts su
         inner join ref_meterpoints mp on mp.account_id = su.external_id and mp.meterpointtype = 'G' and
                                          (least(mp.supplyenddate, mp.associationenddate) is null or
                                           least(mp.supplyenddate, mp.associationenddate) >= current_date)
         inner join ref_meters m
                    on m.account_id = su.external_id and m.meter_point_id = mp.meter_point_id and removeddate is null
--     inner join ref_registers r on r.account_id = su.external_id and r.meter_id = m.meter_id
;

select sum(case when registersattributes_attributevalue = 4 then 1 else 0 end) as fours,
       sum(case when registersattributes_attributevalue = 5 then 1 else 0 end) as fives,
       sum(case when registersattributes_attributevalue = 6 then 1 else 0 end) as sixes
from ref_registers_attributes
where registersattributes_attributename = 'No_Of_Digits'

-- drop table ref_estimated_advance_gas;

create table ref_estimated_advance_gas
(
    account_id            bigint,
    register_id           bigint,
    last_reading_date     timestamp,
    igl_aq                double precision,
    ind_aq                double precision,
    effective_for         timestamp,
    igl_estimated_advance double precision,
    ind_estimated_advance double precision,
    igl_lower_threshold   double precision,
    igl_higher_threshold  double precision,
    ind_lower_threshold   double precision,
    ind_inner_threshold   double precision,
    ind_higher_threshold  double precision,
    etlchange             timestamp
)
    distkey (account_id)
    sortkey (account_id, register_id, effective_for);

alter table ref_estimated_advance_gas
    owner to igloo;

-- drop table ref_estimated_advance_elec;

create table ref_estimated_advance_elec
(
    account_id            bigint,
    register_id           bigint,
    last_reading_date     timestamp,
    igl_eac               double precision,
    ind_eac               double precision,
    effective_for         timestamp,
    igl_estimated_advance double precision,
    ind_estimated_advance double precision,
    igl_lower_threshold   double precision,
    igl_higher_threshold  double precision,
    ind_lower_threshold   double precision,
    ind_higher_threshold  double precision,
    etlchange             timestamp
)
    distkey (account_id)
    sortkey (account_id, register_id, effective_for);

alter table ref_estimated_advance_elec
    owner to igloo;

drop table ref_tolerances;

create table ref_stg_tolerances
(
    group_id         integer distkey,
    tol_group        varchar(255),
    lookup_key       varchar(31),
    lookup_range_min double precision,
    lookup_range_max double precision,
    description      varchar(255),
    tol_min          double precision,
    tol_max          double precision,
    effective_from   timestamp,
    effective_to     timestamp
)
    diststyle key
    sortkey (group_id, effective_from, effective_to)
;

alter table ref_stg_tolerances
    owner to igloo
;



drop table ref_stg_tolerances;

create table dim_tolerances
(
    group_id       integer,
    tol_group      varchar(512),
    look_up_key    integer,
    look_up        varchar(1500),
    min_value      double precision,
    max_value      double precision,
    priority       integer,
    effective_from timestamp,
    effective_to   timestamp
)
    distkey (dummy_key)
    sortkey (id, type, priority);

comment on table dim_tolerances
    is 'A look up table to set tolerances';

alter table dim_tolerances
    owner to igloo;

drop table dim_tolerances;

delete
from dim_tolerances;

insert into dim_tolerances1
select type,
       name,
       '',
       '',
       '',
       '',
       min_value,
       max_value,
       effective_from,
       effective_to
from dim_tolerances;

delete
from ref_stg_tolerances;

insert into ref_stg_tolerances
values (1, 'consumption_accuracy', 'ind_eac_quoted_elec', null, null, null, -0.15, 0.15, '2017-01-01 00:00:00.000000',
        null),
       (1, 'consumption_accuracy', 'ind_aq_quoted_gas', null, null, null, -0.15, 0.15, '2017-01-01 00:00:00.000000',
        null),
       (1, 'consumption_accuracy', 'pa_cons_elec_quoted_elec', null, null, null, -0.15, 0.15,
        '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'pa_cons_elec_ind_eac', null, null, null, -0.15, 0.15, '2017-01-01 00:00:00.000000',
        null),
       (1, 'consumption_accuracy', 'pa_cons_gas_quoted_gas', null, null, null, -0.15, 0.15,
        '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'pa_cons_gas_ind_gas', null, null, null, -0.15, 0.15, '2017-01-01 00:00:00.000000',
        null),
       (1, 'consumption_accuracy', 'igl_ind_eac_pa_cons_elec', null, null, null, -0.15, 0.15,
        '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'igl_ind_aq_pa_cons_gas', null, null, null, -0.15, 0.15,
        '2017-01-01 00:00:00.000000', null),

       (1, 'consumption_accuracy', 'ind_eac', null, null, null, 1000, 35000, '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'igl_ind_eac', null, null, null, 1000, 35000, '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'quotes_eac', null, null, null, 1000, 35000, '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'pa_cons_elec', null, null, null, 3000, 100000, '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'ind_aq', null, null, null, 3000, 100000, '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'igl_ind_aq', null, null, null, 3000, 100000, '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'quotes_aq', null, null, null, 3000, 100000, '2017-01-01 00:00:00.000000', null),
       (1, 'consumption_accuracy', 'pa_cons_gas', null, null, null, 3000, 100000, '2017-01-01 00:00:00.000000', null),

       (3, 'industry_tolerance_gas', 'ind_gas_inner', 1, 1, 'tolerance value in %', 0, 2000000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 2, 100, 'tolerance value in %', 0, 20000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 101, 200, 'tolerance value in %', 0, 10000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 201, 500, 'tolerance value in %', 0, 4000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 501, 1000, 'tolerance value in %', 0, 2000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 1001, 5000, 'tolerance value in %', 0, 400,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 5001, 10000, 'tolerance value in %', 0, 200,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 10001, 20000, 'tolerance value in %', 0, 150,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 20001, 73200, 'tolerance value in %', 0, 300,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 73201, 732000, 'tolerance value in %', 0, 250,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 732001, 2196000, 'tolerance value in %', 0, 200,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_inner', 2196001, 29300000, 'tolerance value in %', 0, 700,
        '2017-01-01 00:00:00.000000', null),

       (3, 'industry_tolerance_gas', 'ind_gas_outer', 1, 1, 'tolerance value in %', 2000000, 7000000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 2, 100, 'tolerance value in %', 20000, 45000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 101, 200, 'tolerance value in %', 10000, 25000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 201, 500, 'tolerance value in %', 4000, 55000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 501, 1000, 'tolerance value in %', 2000, 25000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 1001, 5000, 'tolerance value in %', 400, 7000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 5001, 10000, 'tolerance value in %', 200, 2000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 10001, 20000, 'tolerance value in %', 150, 1100,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 20001, 73200, 'tolerance value in %', 300, 1100,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 73201, 732000, 'tolerance value in %', 250, 1000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 732001, 2196000, 'tolerance value in %', 200, 1000,
        '2017-01-01 00:00:00.000000', null),
       (3, 'industry_tolerance_gas', 'ind_gas_outer', 2196001, 29300000, 'tolerance value in %', 150, 700,
        '2017-01-01 00:00:00.000000', null);

insert into ref_stg_tolerances
values (4, 'industry_tolerance_elec', 'ind_elec', null, null, 'tolerance value in %', 0, 200,
        '2017-01-01 00:00:00.000000', null);

select *
from ref_stg_tolerances;

where type = 3 or name = 'ind_threshold_gas'
order by id;

-- select * from svl_udf_log;

create or replace function convert_kwh_to_cubic(value_kwh double precision, cv double precision,
                                                imp_indicator character)
    returns double precision
    stable
    language plpythonu
as
$$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    u = 1
    value_cubic = 0.0

    # Check if imperial_meter
    if imp_indicator == 'Y':
      u = 2.83

    # Check if cv is 0 then return 0 as it cannot be divided
    if cv != 0:
      value_cubic = (value_kwh * 3.6) / (1.02264 * cv * u)

    return value_cubic

$$;


create or replace function convert_cubic_to_kwh(value_cubic double precision, cv double precision,
                                                imp_indicator character)
    returns double precision
    stable
    language plpythonu
as
$$
    import logging
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    u = 1
    value_kwh = 0.0

    # Check if imperial_meter
    if imp_indicator == 'Y':
      u = 2.83

    # Check if cv is 0 then return 0 as it cannot be divided
    value_kwh = value_cubic * ((1.02264 * cv * u) / 3.6)

    return value_kwh

$$;