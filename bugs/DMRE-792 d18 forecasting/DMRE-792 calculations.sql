-- augment ppc with values taken from 364 days prior
truncate table ref_d18_ppc_forecast;
insert into ref_d18_ppc_forecast
select with_shift.zpd,
       with_shift.shifted_date as st_date,
       with_shift.st_code,
       with_shift.rt_code,
       '0000'                  as run_no,
       with_shift.gsp_group,
       with_shift.gsp,
       with_shift.gsp_group_id,
       with_shift.noon_temp_act,
       with_shift.noon_temp_eff,
       with_shift.time_sunset,
       with_shift.sunset_variable,
       with_shift.pcl,
       with_shift.pcl_id,
       with_shift.pfl,
       with_shift.pfl_id,
       with_shift.ssc,
       with_shift.ss_conf_id,
       with_shift.vmr,
       with_shift.time_pattern_regime,
       with_shift.ppc,
       with_shift.ppc_1,
       with_shift.prsi_1,
       with_shift.ppc_2,
       with_shift.prsi_2,
       with_shift.ppc_3,
       with_shift.prsi_3,
       with_shift.ppc_4,
       with_shift.prsi_4,
       with_shift.ppc_5,
       with_shift.prsi_5,
       with_shift.ppc_6,
       with_shift.prsi_6,
       with_shift.ppc_7,
       with_shift.prsi_7,
       with_shift.ppc_8,
       with_shift.prsi_8,
       with_shift.ppc_9,
       with_shift.prsi_9,
       with_shift.ppc_10,
       with_shift.prsi_10,
       with_shift.ppc_11,
       with_shift.prsi_11,
       with_shift.ppc_12,
       with_shift.prsi_12,
       with_shift.ppc_13,
       with_shift.prsi_13,
       with_shift.ppc_14,
       with_shift.prsi_14,
       with_shift.ppc_15,
       with_shift.prsi_15,
       with_shift.ppc_16,
       with_shift.prsi_16,
       with_shift.ppc_17,
       with_shift.prsi_17,
       with_shift.ppc_18,
       with_shift.prsi_18,
       with_shift.ppc_19,
       with_shift.prsi_19,
       with_shift.ppc_20,
       with_shift.prsi_20,
       with_shift.ppc_21,
       with_shift.prsi_21,
       with_shift.ppc_22,
       with_shift.prsi_22,
       with_shift.ppc_23,
       with_shift.prsi_23,
       with_shift.ppc_24,
       with_shift.prsi_24,
       with_shift.ppc_25,
       with_shift.prsi_25,
       with_shift.ppc_26,
       with_shift.prsi_26,
       with_shift.ppc_27,
       with_shift.prsi_27,
       with_shift.ppc_28,
       with_shift.prsi_28,
       with_shift.ppc_29,
       with_shift.prsi_29,
       with_shift.ppc_30,
       with_shift.prsi_30,
       with_shift.ppc_31,
       with_shift.prsi_31,
       with_shift.ppc_32,
       with_shift.prsi_32,
       with_shift.ppc_33,
       with_shift.prsi_33,
       with_shift.ppc_34,
       with_shift.prsi_34,
       with_shift.ppc_35,
       with_shift.prsi_35,
       with_shift.ppc_36,
       with_shift.prsi_36,
       with_shift.ppc_37,
       with_shift.prsi_37,
       with_shift.ppc_38,
       with_shift.prsi_38,
       with_shift.ppc_39,
       with_shift.prsi_39,
       with_shift.ppc_40,
       with_shift.prsi_40,
       with_shift.ppc_41,
       with_shift.prsi_41,
       with_shift.ppc_42,
       with_shift.prsi_42,
       with_shift.ppc_43,
       with_shift.prsi_43,
       with_shift.ppc_44,
       with_shift.prsi_44,
       with_shift.ppc_45,
       with_shift.prsi_45,
       with_shift.ppc_46,
       with_shift.prsi_46,
       with_shift.ppc_47,
       with_shift.prsi_47,
       with_shift.ppc_48,
       with_shift.prsi_48,
       with_shift.ppc_49,
       with_shift.prsi_49,
       with_shift.ppc_50,
       with_shift.prsi_50
from (select *,
             to_char(dateadd(weeks, 52, to_date(st_date, 'YYYYMMDD')), 'YYYYMMDD') as shifted_date,
             row_number()
             over (partition by st_date,
                 gsp_group_id,
                 pcl_id,
                 pfl_id,
                 ss_conf_id,
                 time_pattern_regime
                 order by run_no desc)                                             as dup_no
      from ref_d18_ppc) with_shift
         left join
     (select st_date, gsp_group_id, pcl_id, pfl_id, ss_conf_id, time_pattern_regime from ref_d18_ppc) no_shift
     on with_shift.shifted_date = no_shift.st_date and
        with_shift.gsp_group_id = no_shift.gsp_group_id and
        with_shift.pcl_id = no_shift.pcl_id and
        with_shift.pfl_id = no_shift.pfl_id and
        with_shift.ss_conf_id = no_shift.ss_conf_id and
        with_shift.time_pattern_regime = no_shift.time_pattern_regime
where no_shift.st_date is null
  and with_shift.dup_no = 1;

-- augment bpp with 364 days prior data
truncate table ref_d18_bpp_forecast;
insert into ref_d18_bpp_forecast
select with_shift.zpd,
       with_shift.shifted_date as st_date,
       with_shift.st_code,
       with_shift.rt_code,
       '0000'                  as run_no,
       with_shift.gsp_group,
       with_shift.gsp,
       with_shift.gsp_group_id,
       with_shift.noon_temp_act,
       with_shift.noon_temp_eff,
       with_shift.time_sunset,
       with_shift.sunset_variable,
       with_shift.pcl,
       with_shift.pcl_id,
       with_shift.pfl,
       with_shift.pfl_id,
       with_shift.bpp,
       with_shift.ppc1,
       with_shift.ppc2,
       with_shift.ppc3,
       with_shift.ppc4,
       with_shift.ppc5,
       with_shift.ppc6,
       with_shift.ppc7,
       with_shift.ppc8,
       with_shift.ppc9,
       with_shift.ppc10,
       with_shift.ppc11,
       with_shift.ppc12,
       with_shift.ppc13,
       with_shift.ppc14,
       with_shift.ppc15,
       with_shift.ppc16,
       with_shift.ppc17,
       with_shift.ppc18,
       with_shift.ppc19,
       with_shift.ppc20,
       with_shift.ppc21,
       with_shift.ppc22,
       with_shift.ppc23,
       with_shift.ppc24,
       with_shift.ppc25,
       with_shift.ppc26,
       with_shift.ppc27,
       with_shift.ppc28,
       with_shift.ppc29,
       with_shift.ppc30,
       with_shift.ppc31,
       with_shift.ppc32,
       with_shift.ppc33,
       with_shift.ppc34,
       with_shift.ppc35,
       with_shift.ppc36,
       with_shift.ppc37,
       with_shift.ppc38,
       with_shift.ppc39,
       with_shift.ppc40,
       with_shift.ppc41,
       with_shift.ppc42,
       with_shift.ppc43,
       with_shift.ppc44,
       with_shift.ppc45,
       with_shift.ppc46,
       with_shift.ppc47,
       with_shift.ppc48,
       with_shift.ppc49,
       with_shift.ppc50
from (select *,
             to_char(dateadd(weeks, 52, to_date(st_date, 'YYYYMMDD')), 'YYYYMMDD') as shifted_date,
             row_number()
             over (partition by st_date,
                 gsp_group_id,
                 pcl_id,
                 pfl_id
                 order by run_no desc)                                             as dup_no
      from ref_d18_bpp) with_shift
         left join
         (select st_date, gsp_group_id, pcl_id, pfl_id from ref_d18_bpp) no_shift
         on with_shift.shifted_date = no_shift.st_date and
            with_shift.gsp_group_id = no_shift.gsp_group_id and
            with_shift.pcl_id = no_shift.pcl_id and
            with_shift.pfl_id = no_shift.pfl_id
where no_shift.st_date is null
  and with_shift.dup_no = 1;




select with_shift.zpd,
       with_shift.shifted_date as st_date,
       with_shift.st_code,
       with_shift.rt_code,
       '0000'                  as run_no,
       with_shift.gsp_group,
       with_shift.gsp,
       with_shift.gsp_group_id,
       with_shift.noon_temp_act,
       with_shift.noon_temp_eff,
       with_shift.time_sunset,
       with_shift.sunset_variable,
       with_shift.pcl,
       with_shift.pcl_id,
       with_shift.pfl,
       with_shift.pfl_id,
       with_shift.bpp,
       with_shift.bpp_sum,
       with_shift.row,
       getdate()               as etlchange
from (select *,
             to_char(dateadd(weeks, 52, to_date(st_date, 'YYYYMMDD')), 'YYYYMMDD') as shifted_date,
             row_number()
             over (partition by st_date,
                 gsp_group_id,
                 pcl_id,
                 pfl_id
                 order by run_no desc)                                             as dup_no
      from ref_d18_igloo_bpp) with_shift
         left join
         (select st_date, gsp_group_id, pcl_id, pfl_id from ref_d18_igloo_bpp) no_shift
         on with_shift.shifted_date = no_shift.st_date and
            with_shift.gsp_group_id = no_shift.gsp_group_id and
            with_shift.pcl_id = no_shift.pcl_id and
            with_shift.pfl_id = no_shift.pfl_id
where no_shift.st_date is null
  and with_shift.dup_no = 1


-- igloo ppc
select with_shift.zpd,
       with_shift.shifted_date as st_date,
       with_shift.st_code,
       with_shift.rt_code,
       '0000'                  as run_no,
       with_shift.gsp_group,
       with_shift.gsp,
       with_shift.gsp_group_id,
       with_shift.noon_temp_act,
       with_shift.noon_temp_eff,
       with_shift.time_sunset,
       with_shift.sunset_variable,
       with_shift.pcl,
       with_shift.pcl_id,
       with_shift.pfl,
       with_shift.pfl_id,
       with_shift.ssc,
       with_shift.ss_conf_id,
       with_shift.vmr,
       with_shift.time_pattern_regime,
       with_shift.ppc,
       with_shift.ppc_sum,
       with_shift.row,
       getdate()               as etlchange
from (select *,
             to_char(dateadd(weeks, 52, to_date(st_date, 'YYYYMMDD')), 'YYYYMMDD') as shifted_date,
             row_number()
             over (partition by st_date,
                 gsp_group_id,
                 pcl_id,
                 pfl_id,
                 ss_conf_id,
                 time_pattern_regime
                 order by run_no desc)                                             as dup_no
      from ref_d18_igloo_ppc) with_shift
         left join
     (select st_date, gsp_group_id, pcl_id, pfl_id, ss_conf_id, time_pattern_regime from ref_d18_igloo_ppc) no_shift
     on with_shift.shifted_date = no_shift.st_date and
        with_shift.gsp_group_id = no_shift.gsp_group_id and
        with_shift.pcl_id = no_shift.pcl_id and
        with_shift.pfl_id = no_shift.pfl_id and
        with_shift.ss_conf_id = no_shift.ss_conf_id and
        with_shift.time_pattern_regime = no_shift.time_pattern_regime
where no_shift.st_date is null
  and with_shift.dup_no = 1;



select count(*)
from ref_d18_bpp_forecast

select count(*)
from ref_d18_ppc_forecast;


