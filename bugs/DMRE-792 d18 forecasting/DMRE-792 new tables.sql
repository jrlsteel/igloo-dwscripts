
-- New tables
-- auto-generated definition from ref_d18_bpp
drop table ref_d18_bpp_forecast;
create table ref_d18_bpp_forecast
(
    zpd             varchar(10),
    st_date         varchar(10) encode bytedict distkey,
    st_code         varchar(10),
    rt_code         varchar(10),
    run_no          varchar(255),
    gsp_group       varchar(255),
    gsp             varchar(10),
    gsp_group_id    varchar(10),
    noon_temp_act   double precision encode bytedict,
    noon_temp_eff   double precision encode bytedict,
    time_sunset     varchar(255),
    sunset_variable varchar(255),
    pcl             varchar(10),
    pcl_id          integer,
    pfl             varchar(10),
    pfl_id          integer,
    bpp             varchar(10),
    ppc1            double precision,
    ppc2            double precision,
    ppc3            double precision,
    ppc4            double precision,
    ppc5            double precision,
    ppc6            double precision,
    ppc7            double precision,
    ppc8            double precision,
    ppc9            double precision,
    ppc10           double precision,
    ppc11           double precision,
    ppc12           double precision,
    ppc13           double precision,
    ppc14           double precision,
    ppc15           double precision,
    ppc16           double precision,
    ppc17           double precision,
    ppc18           double precision,
    ppc19           double precision,
    ppc20           double precision,
    ppc21           double precision,
    ppc22           double precision,
    ppc23           double precision,
    ppc24           double precision,
    ppc25           double precision,
    ppc26           double precision,
    ppc27           double precision,
    ppc28           double precision,
    ppc29           double precision,
    ppc30           double precision,
    ppc31           double precision,
    ppc32           double precision,
    ppc33           double precision,
    ppc34           double precision,
    ppc35           double precision,
    ppc36           double precision,
    ppc37           double precision,
    ppc38           double precision,
    ppc39           double precision,
    ppc40           double precision,
    ppc41           double precision,
    ppc42           double precision,
    ppc43           double precision,
    ppc44           double precision,
    ppc45           double precision,
    ppc46           double precision,
    ppc47           double precision,
    ppc48           double precision,
    ppc49           double precision,
    ppc50           double precision
)
    diststyle key
    sortkey (st_date);

alter table ref_d18_bpp_forecast
    owner to igloo;

-- auto-generated definition from ref_d18_ppc
drop table ref_d18_ppc_forecast;
create table ref_d18_ppc_forecast
(
    zpd                 varchar(10),
    st_date             varchar(10) distkey,
    st_code             varchar(10),
    rt_code             varchar(10),
    run_no              varchar(255),
    gsp_group           varchar(255),
    gsp                 varchar(10),
    gsp_group_id        varchar(10),
    noon_temp_act       double precision encode bytedict,
    noon_temp_eff       double precision encode bytedict,
    time_sunset         varchar(255),
    sunset_variable     varchar(255),
    pcl                 varchar(10),
    pcl_id              integer encode delta,
    pfl                 varchar(10),
    pfl_id              integer encode delta,
    ssc                 varchar(10),
    ss_conf_id          varchar(10),
    vmr                 varchar(10),
    time_pattern_regime varchar(255),
    ppc                 varchar(10),
    ppc_1               double precision encode bytedict,
    prsi_1              varchar(10) encode bytedict,
    ppc_2               double precision encode bytedict,
    prsi_2              varchar(10) encode bytedict,
    ppc_3               double precision encode bytedict,
    prsi_3              varchar(10) encode bytedict,
    ppc_4               double precision encode bytedict,
    prsi_4              varchar(10) encode bytedict,
    ppc_5               double precision encode bytedict,
    prsi_5              varchar(10) encode bytedict,
    ppc_6               double precision encode bytedict,
    prsi_6              varchar(10) encode bytedict,
    ppc_7               double precision encode bytedict,
    prsi_7              varchar(10) encode bytedict,
    ppc_8               double precision encode bytedict,
    prsi_8              varchar(10) encode bytedict,
    ppc_9               double precision encode bytedict,
    prsi_9              varchar(10) encode bytedict,
    ppc_10              double precision encode bytedict,
    prsi_10             varchar(10) encode bytedict,
    ppc_11              double precision encode bytedict,
    prsi_11             varchar(10) encode bytedict,
    ppc_12              double precision encode bytedict,
    prsi_12             varchar(10) encode bytedict,
    ppc_13              double precision encode bytedict,
    prsi_13             varchar(10) encode bytedict,
    ppc_14              double precision encode bytedict,
    prsi_14             varchar(10) encode bytedict,
    ppc_15              double precision encode bytedict,
    prsi_15             varchar(10) encode bytedict,
    ppc_16              double precision encode bytedict,
    prsi_16             varchar(10) encode bytedict,
    ppc_17              double precision encode bytedict,
    prsi_17             varchar(10) encode bytedict,
    ppc_18              double precision encode bytedict,
    prsi_18             varchar(10) encode bytedict,
    ppc_19              double precision encode bytedict,
    prsi_19             varchar(10) encode bytedict,
    ppc_20              double precision encode bytedict,
    prsi_20             varchar(10) encode bytedict,
    ppc_21              double precision encode bytedict,
    prsi_21             varchar(10) encode bytedict,
    ppc_22              double precision encode bytedict,
    prsi_22             varchar(10) encode bytedict,
    ppc_23              double precision encode bytedict,
    prsi_23             varchar(10) encode bytedict,
    ppc_24              double precision encode bytedict,
    prsi_24             varchar(10) encode bytedict,
    ppc_25              double precision encode bytedict,
    prsi_25             varchar(10) encode bytedict,
    ppc_26              double precision encode bytedict,
    prsi_26             varchar(10) encode bytedict,
    ppc_27              double precision encode bytedict,
    prsi_27             varchar(10) encode bytedict,
    ppc_28              double precision encode bytedict,
    prsi_28             varchar(10) encode bytedict,
    ppc_29              double precision encode bytedict,
    prsi_29             varchar(10) encode bytedict,
    ppc_30              double precision encode bytedict,
    prsi_30             varchar(10) encode bytedict,
    ppc_31              double precision encode bytedict,
    prsi_31             varchar(10) encode bytedict,
    ppc_32              double precision encode bytedict,
    prsi_32             varchar(10) encode bytedict,
    ppc_33              double precision encode bytedict,
    prsi_33             varchar(10) encode bytedict,
    ppc_34              double precision encode bytedict,
    prsi_34             varchar(10) encode bytedict,
    ppc_35              double precision encode bytedict,
    prsi_35             varchar(10) encode bytedict,
    ppc_36              double precision encode bytedict,
    prsi_36             varchar(10) encode bytedict,
    ppc_37              double precision encode bytedict,
    prsi_37             varchar(10) encode bytedict,
    ppc_38              double precision encode bytedict,
    prsi_38             varchar(10) encode bytedict,
    ppc_39              double precision encode bytedict,
    prsi_39             varchar(10) encode bytedict,
    ppc_40              double precision encode bytedict,
    prsi_40             varchar(10) encode bytedict,
    ppc_41              double precision encode bytedict,
    prsi_41             varchar(10) encode bytedict,
    ppc_42              double precision encode bytedict,
    prsi_42             varchar(10) encode bytedict,
    ppc_43              double precision encode bytedict,
    prsi_43             varchar(10) encode bytedict,
    ppc_44              double precision encode bytedict,
    prsi_44             varchar(10) encode bytedict,
    ppc_45              double precision encode bytedict,
    prsi_45             varchar(10) encode bytedict,
    ppc_46              double precision encode bytedict,
    prsi_46             varchar(10) encode bytedict,
    ppc_47              double precision encode bytedict,
    prsi_47             varchar(10) encode bytedict,
    ppc_48              double precision encode bytedict,
    prsi_48             varchar(10) encode bytedict,
    ppc_49              double precision encode bytedict,
    prsi_49             varchar(10),
    ppc_50              double precision encode bytedict,
    prsi_50             varchar(10)
)
    diststyle key
    sortkey (st_date);

alter table ref_d18_ppc_forecast
    owner to igloo;

-- auto-generated definition from ref_d18_igloo_bpp
drop table ref_d18_igloo_bpp_forecast;
create table ref_d18_igloo_bpp_forecast
(
    zpd             varchar(10),
    st_date         timestamp distkey,
    st_code         varchar(10),
    rt_code         varchar(10),
    run_no          varchar(255),
    gsp_group       varchar(255),
    gsp             varchar(10),
    gsp_group_id    varchar(10),
    noon_temp_act   double precision,
    noon_temp_eff   double precision,
    time_sunset     varchar(255),
    sunset_variable varchar(255),
    pcl             varchar(10),
    pcl_id          integer,
    pfl             varchar(10),
    pfl_id          integer,
    bpp             varchar(10),
    bpp_sum         double precision,
    row             bigint,
    etlchange       timestamp
)
    diststyle key
    sortkey (st_date);

alter table ref_d18_igloo_bpp_forecast
    owner to igloo;

-- auto-generated definition from ref_d18_igloo_ppc
drop table ref_d18_igloo_ppc_forecast;
create table ref_d18_igloo_ppc_forecast
(
    zpd                 varchar(10),
    st_date             timestamp,
    st_code             varchar(10),
    rt_code             varchar(10),
    run_no              varchar(255),
    gsp_group           varchar(255),
    gsp                 varchar(10),
    gsp_group_id        varchar(10),
    noon_temp_act       double precision encode bytedict,
    noon_temp_eff       double precision encode bytedict,
    time_sunset         varchar(255),
    sunset_variable     varchar(255),
    pcl                 varchar(10),
    pcl_id              integer encode delta,
    pfl                 varchar(10),
    pfl_id              integer encode delta,
    ssc                 varchar(10),
    ss_conf_id          varchar(10) distkey,
    vmr                 varchar(10),
    time_pattern_regime varchar(255),
    ppc                 varchar(10),
    ppc_sum             double precision,
    row                 bigint,
    etlchange           timestamp
)
    diststyle key
    sortkey (st_date);

alter table ref_d18_igloo_ppc_forecast
    owner to igloo;
