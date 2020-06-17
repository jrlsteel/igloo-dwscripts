/* Redshift */
drop table ref_eac_calc_params;
create table ref_eac_calc_params
(
    account_id            bigint distkey,
    meter_point_id        bigint,
    meter_id              bigint,
    register_id           bigint,
    meterpointnumber      bigint,
    meter_serial          varchar(20),
    gsp                   varchar(2),
    ssc                   varchar(10),
    tpr                   bigint,
    pcl                   varchar(10),
    num_dials             integer,
    smooth_param          integer,
    first_read_date       timestamp,
    first_read_value      double precision,
    first_read_cppc       double precision,
    first_read_cppc_count bigint,
    prev_igl_ind_eac      double precision,
    prev_read_date        timestamp,
    prev_read_value       double precision,
    prev_read_cppc        double precision,
    prev_read_cppc_count  bigint,
    today_cppc            double precision,
    today_cppc_count      bigint,
    etlchange             timestamp
)
    diststyle key;
alter table ref_eac_calc_params
    owner to igloo;


/* Aurora */
drop table ref_eac_calc_params;
create table ref_eac_calc_params
(
    account_id            bigint,
    meter_point_id        bigint,
    meter_id              bigint,
    register_id           bigint,
    meterpointnumber      bigint,
    meter_serial          varchar(20),
    gsp                   varchar(2),
    ssc                   varchar(10),
    tpr                   bigint,
    pcl                   varchar(10),
    num_dials             integer,
    smooth_param          integer,
    first_read_date       timestamp,
    first_read_value      double precision,
    first_read_cppc       double precision,
    first_read_cppc_count bigint,
    prev_igl_ind_eac      double precision,
    prev_read_date        timestamp,
    prev_read_value       double precision,
    prev_read_cppc        double precision,
    prev_read_cppc_count  bigint,
    today_cppc            double precision,
    today_cppc_count      bigint,
    etlchange             timestamp
);

drop table temp_eac_calc_params
create table temp_eac_calc_params as
truncate table ref_eac_calc_params;
insert into ref_eac_calc_params
with cte_readings as (select account_id,
                             meter_point_id,
                             register_id,
                             meterreadingdatetime,
                             min(readingvalue) as readingvalue
                      from ref_readings_internal_valid
                      group by account_id,
                               meter_point_id,
                               register_id,
                               meterreadingdatetime)
select mp.account_id,
       mp.meter_point_id,
       met.meter_id,
       reg.register_id,
       mp.meterpointnumber,
       met.meterserialnumber                                      as meter_serial,
       iie.elec_gsp                                               as gsp,
       iie.elec_ssc                                               as ssc,
       reg.registers_tpr::bigint                                  as tpr,
       iie.profile_class                                          as pcl,
       iie.no_of_digits                                           as num_dials,
       2                                                          as smooth_param,
       first_read.meterreadingdatetime                            as first_read_date,
       first_read.readingvalue                                    as first_read_value,
       first_cppc.cumulative_ppc                                  as first_read_cppc,
       first_cppc.cumulative_ppc_count                            as first_read_cppc_count,
       nvl(nullif(iie.igl_ind_eac, 0), iie.industry_eac_register) as prev_igl_ind_eac,
       prev_read.meterreadingdatetime                             as prev_read_date,
       prev_read.readingvalue                                     as prev_read_value,
       prev_cppc.cumulative_ppc                                   as prev_read_cppc,
       prev_cppc.cumulative_ppc_count                             as prev_read_cppc_count,
       today_cppc.cumulative_ppc                                  as today_cppc,
       today_cppc.cumulative_ppc_count                            as today_cppc_count,
       getdate()                                                  as etlchange
from ref_meterpoints mp
         inner join ref_meters met on mp.account_id = met.account_id and
                                      mp.meter_point_id = met.meter_point_id and
                                      met.removeddate is null
         inner join ref_registers reg on reg.account_id = met.account_id and
                                         reg.meter_point_id = met.meter_point_id and
                                         reg.meter_id = met.meter_id and
                                         reg.registers_tprperioddescription is not null
         left join ref_calculated_igl_ind_eac iie on iie.account_id = mp.account_id and
                                                     iie.meterpoint_id = mp.meter_point_id and
                                                     iie.register_id = reg.register_id
         left join cte_readings first_read on iie.account_id = first_read.account_id and
                                              iie.meterpoint_id = first_read.meter_point_id and
                                              iie.register_id = first_read.register_id and
                                              trunc(iie.read_min_datetime_elec) =
                                              trunc(first_read.meterreadingdatetime)
         left join cte_readings prev_read on iie.account_id = prev_read.account_id and
                                             iie.meterpoint_id = prev_read.meter_point_id and
                                             iie.register_id = prev_read.register_id and
                                             trunc(iie.read_max_datetime_elec) =
                                             trunc(prev_read.meterreadingdatetime)
         left join ref_cumulative_ppc first_cppc
                   on trunc(first_cppc.ppc_date) = trunc(first_read.meterreadingdatetime) and
                      first_cppc.gsp = iie.elec_gsp and
                      first_cppc.ssc = iie.elec_ssc and
                      first_cppc.tpr = reg.registers_tpr::bigint and
                      first_cppc.pcl = iie.profile_class
         left join ref_cumulative_ppc prev_cppc on trunc(prev_cppc.ppc_date) = trunc(prev_read.meterreadingdatetime) and
                                                   prev_cppc.gsp = iie.elec_gsp and
                                                   prev_cppc.ssc = iie.elec_ssc and
                                                   prev_cppc.tpr = reg.registers_tpr::bigint and
                                                   prev_cppc.pcl = iie.profile_class
         left join ref_cumulative_ppc today_cppc on trunc(today_cppc.ppc_date) = trunc(getdate()) and
                                                    today_cppc.gsp = iie.elec_gsp and
                                                    today_cppc.ssc = iie.elec_ssc and
                                                    today_cppc.tpr = reg.registers_tpr::bigint and
                                                    today_cppc.pcl = iie.profile_class
where nvl(least(mp.supplyenddate, mp.associationenddate), getdate() + 1) > getdate()
  and mp.meterpointtype = 'E'


select *
from ref_calculated_daily_customer_file dcf
         left join ref_calculated_igl_ind_eac iie on dcf.account_id = iie.account_id
where dcf.elec_reg_status = 'Live'
  and iie.account_id is null


drop table ref_aq_calc_params;
create table ref_aq_calc_params
(
    account_id        bigint distkey,
    meter_point_id    bigint,
    meter_id          bigint,
    register_id       bigint,
    meterpointnumber  bigint,
    meter_serial      varchar(20),
    ldz               varchar(2),
    num_dials         integer,
    imperial          boolean,
    first_read_date   timestamp,
    first_read_value  double precision,
    prev_igl_ind_aq   double precision,
    prev_read_date    timestamp,
    today_cwaalp      double precision,
    today_waalp_count bigint,
    today_ccv         double precision,
    today_cv_count    bigint,
    etlchange         timestamp
)
    diststyle key
    sortkey (account_id, meter_point_id, meter_id, register_id);

alter table ref_aq_calc_params
    owner to igloo;

-- auto-generated definition
drop table ref_cumulative_alp_cv;
create table ref_cumulative_alp_cv
(
    coeff_date  timestamp,
    ldz         varchar(2) distkey,
    cwaalp      double precision,
    waalp_count bigint,
    ccv         double precision,
    cv_count    bigint
)
    diststyle key
    sortkey (coeff_date, ldz);

alter table ref_cumulative_alp_cv
    owner to igloo;


/* aurora */
create table ref_aq_calc_params
(
    account_id        bigint,
    meter_point_id    bigint,
    meter_id          bigint,
    register_id       bigint,
    meterpointnumber  bigint,
    meter_serial      varchar(20),
    ldz               varchar(2),
    num_dials         integer,
    imperial          boolean,
    first_read_date   timestamp,
    first_read_value  double precision,
    prev_igl_ind_aq   double precision,
    prev_read_date    timestamp,
    today_cwaalp      double precision,
    today_waalp_count bigint,
    today_ccv         double precision,
    today_cv_count    bigint,
    etlchange         timestamp
);

create table ref_cumulative_alp_cv
(
    coeff_date  timestamp,
    ldz         varchar(2),
    cwaalp      double precision,
    waalp_count bigint,
    ccv         double precision,
    cv_count    bigint
);
