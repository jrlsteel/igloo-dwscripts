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
    diststyle key
    sortkey (account_id, meter_point_id, meter_id, register_id);

alter table ref_eac_calc_params
    owner to igloo;


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


create table ref_cumulative_ppc
(
    ppc_date             date,
    gsp                  varchar(10) distkey,
    ssc                  varchar(10),
    tpr                  bigint,
    pcl                  integer,
    cumulative_ppc       double precision,
    cumulative_ppc_count bigint
)
    diststyle key
    sortkey (ppc_date, gsp, ssc, tpr, pcl);

alter table ref_cumulative_ppc
    owner to igloo;

