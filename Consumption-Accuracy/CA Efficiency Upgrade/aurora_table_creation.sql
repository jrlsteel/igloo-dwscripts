create table ref_aq_calc_params
(
    account_id        bigint      null,
    meter_point_id    bigint      null,
    meter_id          bigint      null,
    register_id       bigint      null,
    meterpointnumber  bigint      null,
    meter_serial      varchar(20) null,
    ldz               varchar(2)  null,
    num_dials         int         null,
    imperial          tinyint(1)  null,
    first_read_date   timestamp   null,
    first_read_value  double      null,
    prev_igl_ind_aq   double      null,
    prev_read_date    timestamp   null,
    today_cwaalp      double      null,
    today_waalp_count bigint      null,
    today_ccv         double      null,
    today_cv_count    bigint      null,
    etlchange         timestamp   null
);

create table ref_eac_calc_params
(
    account_id            bigint      null,
    meter_point_id        bigint      null,
    meter_id              bigint      null,
    register_id           bigint      null,
    meterpointnumber      bigint      null,
    meter_serial          varchar(20) null,
    gsp                   varchar(2)  null,
    ssc                   varchar(10) null,
    tpr                   bigint      null,
    pcl                   varchar(10) null,
    num_dials             int         null,
    smooth_param          int         null,
    first_read_date       timestamp   null,
    first_read_value      double      null,
    first_read_cppc       double      null,
    first_read_cppc_count bigint      null,
    prev_igl_ind_eac      double      null,
    prev_read_date        timestamp   null,
    prev_read_value       double      null,
    prev_read_cppc        double      null,
    prev_read_cppc_count  bigint      null,
    today_cppc            double      null,
    today_cppc_count      bigint      null,
    etlchange             timestamp   null
);

create table ref_cumulative_ppc
(
    ppc_date             date        null,
    gsp                  varchar(10) null,
    ssc                  varchar(10) null,
    tpr                  bigint      null,
    pcl                  int         null,
    cumulative_ppc       double      null,
    cumulative_ppc_count bigint      null
);

create table ref_cumulative_alp_cv
(
    coeff_date  timestamp  null,
    ldz         varchar(2) null,
    cwaalp      double     null,
    waalp_count bigint     null,
    ccv         double     null,
    cv_count    bigint     null
);

