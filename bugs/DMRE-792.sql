select distinct right(time_sunset, 2)
from ref_d18_igloo_bpp;

select st_month + '-' + st_day                      as st_date,
       gsp_group_id,
       pcl_id,
       pfl_id,
       round(noon_temp_act, 1)                      as noon_temp_act,
       round(noon_temp_eff, 1)                      as noon_temp_eff,
       lpad(floor(avg_sunset / 60), 2, '0') +
       lpad(floor(mod(avg_sunset, 60)), 2, '0') +
       lpad(floor(mod(avg_sunset, 1) * 60), 2, '0') as time_sunset,
       round(sunset_variable, 1)                    as sunset_variable,
       bpp_sum,
       avg_over,
       etlchange
from (select to_char(st_date, 'MM')                                                      as st_month,
             to_char(st_date, 'DD')                                                      as st_day,
             gsp_group_id                                                                as gsp_group_id,
             avg(noon_temp_act)                                                          as noon_temp_act,
             avg(noon_temp_eff)                                                          as noon_temp_eff,
             avg((left(time_sunset, 2)::int * 60) + left(right(time_sunset, 4), 2)::int) as avg_sunset,
             avg(sunset_variable::int)                                                   as sunset_variable,
             pcl_id                                                                      as pcl_id,
             pfl_id                                                                      as pfl_id,
             avg(bpp_sum)                                                                as bpp_sum,
             count(*)                                                                    as avg_over,
             current_timestamp                                                           as etlchange
      from ref_d18_igloo_bpp
      group by st_month, st_day, gsp_group_id, pcl_id, pfl_id
      order by gsp_group_id, pcl_id, pfl_id, st_month, st_day
     ) bpp_avg
;

select distinct ss_conf_id
from ref_d18_ppc

select lpad(floor(1208.9 / 60), 2, '0') +
       lpad(floor(mod(1208.9, 60)), 2, '0') +
       lpad(floor(mod(1208.9, 1) * 60), 2, '0')

select to_char(st_date, 'MM')                                                      as st_month,
       to_char(st_date, 'DD')                                                      as st_day,
       gsp_group_id,
       pcl_id,
       pfl_id,
       ss_conf_id,
       time_pattern_regime,
       avg(noon_temp_act),
       avg(noon_temp_eff),
       avg((left(time_sunset, 2)::int * 60) + left(right(time_sunset, 4), 2)::int) as avg_sunset,
       avg(sunset_variable::int)
--,
/*
*/
;
drop table temp_avg_ppc;
create table temp_avg_ppc as
select --left(right(st_date, 4), 2) + '-' + right(st_date, 2) as st_monthday,
       date_part(week,to_date(st_date,'YYYYMMDD'))::varchar(2) + '-' +
       date_part(weekday,to_date(st_date,'YYYYMMDD'))::char as st_week_day,
       gsp_group_id,
       pcl_id,
       pfl_id,
       ss_conf_id,
       time_pattern_regime,
       count(*)                                                                   as avg_over,
       round(avg(noon_temp_act), 1)                                               as noon_temp_act,
       round(avg(noon_temp_eff), 1)                                               as noon_temp_eff,
       to_char(
               to_timestamp(
                       avg((left(time_sunset, 2)::int * 60) +
                           left(right(time_sunset, 4), 2)::int),
                       'MI'),
               'HHMISS')                                                          as avg_sunset,
       avg(sunset_variable::int)                                                  as sunset_variable,
       avg(ppc_1)                                                                 as ppc_1,
       listagg(distinct prsi_1, ',')                                              as prsi_1,
       avg(ppc_2)                                                                 as ppc_2,
       listagg(distinct prsi_2, ',')                                              as prsi_2,
       avg(ppc_3)                                                                 as ppc_3,
       listagg(distinct prsi_3, ',')                                              as prsi_3,
       avg(ppc_4)                                                                 as ppc_4,
       listagg(distinct prsi_4, ',')                                              as prsi_4,
       avg(ppc_5)                                                                 as ppc_5,
       listagg(distinct prsi_5, ',')                                              as prsi_5,
       avg(ppc_6)                                                                 as ppc_6,
       listagg(distinct prsi_6, ',')                                              as prsi_6,
       avg(ppc_7)                                                                 as ppc_7,
       listagg(distinct prsi_7, ',')                                              as prsi_7,
       avg(ppc_8)                                                                 as ppc_8,
       listagg(distinct prsi_8, ',')                                              as prsi_8,
       avg(ppc_9)                                                                 as ppc_9,
       listagg(distinct prsi_9, ',')                                              as prsi_9,
       avg(ppc_10)                                                                as ppc_10,
       listagg(distinct prsi_10, ',')                                             as prsi_10,
       avg(ppc_11)                                                                as ppc_11,
       listagg(distinct prsi_11, ',')                                             as prsi_11,
       avg(ppc_12)                                                                as ppc_12,
       listagg(distinct prsi_12, ',')                                             as prsi_12,
       avg(ppc_13)                                                                as ppc_13,
       listagg(distinct prsi_13, ',')                                             as prsi_13,
       avg(ppc_14)                                                                as ppc_14,
       listagg(distinct prsi_14, ',')                                             as prsi_14,
       avg(ppc_15)                                                                as ppc_15,
       listagg(distinct prsi_15, ',')                                             as prsi_15,
       avg(ppc_16)                                                                as ppc_16,
       listagg(distinct prsi_16, ',')                                             as prsi_16,
       avg(ppc_17)                                                                as ppc_17,
       listagg(distinct prsi_17, ',')                                             as prsi_17,
       avg(ppc_18)                                                                as ppc_18,
       listagg(distinct prsi_18, ',')                                             as prsi_18,
       avg(ppc_19)                                                                as ppc_19,
       listagg(distinct prsi_19, ',')                                             as prsi_19,
       avg(ppc_20)                                                                as ppc_20,
       listagg(distinct prsi_20, ',')                                             as prsi_20,
       avg(ppc_21)                                                                as ppc_21,
       listagg(distinct prsi_21, ',')                                             as prsi_21,
       avg(ppc_22)                                                                as ppc_22,
       listagg(distinct prsi_22, ',')                                             as prsi_22,
       avg(ppc_23)                                                                as ppc_23,
       listagg(distinct prsi_23, ',')                                             as prsi_23,
       avg(ppc_24)                                                                as ppc_24,
       listagg(distinct prsi_24, ',')                                             as prsi_24,
       avg(ppc_25)                                                                as ppc_25,
       listagg(distinct prsi_25, ',')                                             as prsi_25,
       avg(ppc_26)                                                                as ppc_26,
       listagg(distinct prsi_26, ',')                                             as prsi_26,
       avg(ppc_27)                                                                as ppc_27,
       listagg(distinct prsi_27, ',')                                             as prsi_27,
       avg(ppc_28)                                                                as ppc_28,
       listagg(distinct prsi_28, ',')                                             as prsi_28,
       avg(ppc_29)                                                                as ppc_29,
       listagg(distinct prsi_29, ',')                                             as prsi_29,
       avg(ppc_30)                                                                as ppc_30,
       listagg(distinct prsi_30, ',')                                             as prsi_30,
       avg(ppc_31)                                                                as ppc_31,
       listagg(distinct prsi_31, ',')                                             as prsi_31,
       avg(ppc_32)                                                                as ppc_32,
       listagg(distinct prsi_32, ',')                                             as prsi_32,
       avg(ppc_33)                                                                as ppc_33,
       listagg(distinct prsi_33, ',')                                             as prsi_33,
       avg(ppc_34)                                                                as ppc_34,
       listagg(distinct prsi_34, ',')                                             as prsi_34,
       avg(ppc_35)                                                                as ppc_35,
       listagg(distinct prsi_35, ',')                                             as prsi_35,
       avg(ppc_36)                                                                as ppc_36,
       listagg(distinct prsi_36, ',')                                             as prsi_36,
       avg(ppc_37)                                                                as ppc_37,
       listagg(distinct prsi_37, ',')                                             as prsi_37,
       avg(ppc_38)                                                                as ppc_38,
       listagg(distinct prsi_38, ',')                                             as prsi_38,
       avg(ppc_39)                                                                as ppc_39,
       listagg(distinct prsi_39, ',')                                             as prsi_39,
       avg(ppc_40)                                                                as ppc_40,
       listagg(distinct prsi_40, ',')                                             as prsi_40,
       avg(ppc_41)                                                                as ppc_41,
       listagg(distinct prsi_41, ',')                                             as prsi_41,
       avg(ppc_42)                                                                as ppc_42,
       listagg(distinct prsi_42, ',')                                             as prsi_42,
       avg(ppc_43)                                                                as ppc_43,
       listagg(distinct prsi_43, ',')                                             as prsi_43,
       avg(ppc_44)                                                                as ppc_44,
       listagg(distinct prsi_44, ',')                                             as prsi_44,
       avg(ppc_45)                                                                as ppc_45,
       listagg(distinct prsi_45, ',')                                             as prsi_45,
       avg(ppc_46)                                                                as ppc_46,
       listagg(distinct prsi_46, ',')                                             as prsi_46,
       avg(ppc_47)                                                                as ppc_47,
       listagg(distinct prsi_47, ',')                                             as prsi_47,
       avg(ppc_48)                                                                as ppc_48,
       listagg(distinct prsi_48, ',')                                             as prsi_48,
       avg(ppc_49)                                                                as ppc_49,
       listagg(distinct prsi_49, ',')                                             as prsi_49,
       avg(ppc_50)                                                                as ppc_50,
       listagg(distinct prsi_50, ',')                                             as prsi_50
from ref_d18_ppc
group by st_week_day,
         gsp_group_id,
         pcl_id,
         pfl_id,
         ss_conf_id,
         time_pattern_regime
order by st_week_day,
         gsp_group_id,
         pcl_id,
         pfl_id,
         ss_conf_id,
         time_pattern_regime

select regexp_count(st_date, '\\.') as dec_count
from ref_d18_ppc
where dec_count > 1

select to_char(to_timestamp('2345', 'MI'), 'HHMISS')

select *,
       left(right(st_date, 4), 2) + '-' + right(st_date, 2) as st_monthday
from ref_d18_ppc
where st_monthday = '01-02'
  and gsp_group_id = '_A'
  and pcl_id = 1
  and pfl_id = 1
  and ss_conf_id = '0464'
  and time_pattern_regime = '00365'


select distinct st_week_day
from temp_avg_ppc
where gsp_group_id != '_N'
  and (prsi_1 in ('T,F', 'F,T') or prsi_2 in ('T,F', 'F,T') or prsi_3 in ('T,F', 'F,T') or prsi_4 in ('T,F', 'F,T') or
       prsi_5 in ('T,F', 'F,T') or prsi_6 in ('T,F', 'F,T') or prsi_7 in ('T,F', 'F,T') or prsi_8 in ('T,F', 'F,T') or
       prsi_9 in ('T,F', 'F,T') or prsi_10 in ('T,F', 'F,T') or prsi_11 in ('T,F', 'F,T') or
       prsi_12 in ('T,F', 'F,T') or
       prsi_13 in ('T,F', 'F,T') or prsi_14 in ('T,F', 'F,T') or prsi_15 in ('T,F', 'F,T') or
       prsi_16 in ('T,F', 'F,T') or
       prsi_17 in ('T,F', 'F,T') or prsi_18 in ('T,F', 'F,T') or prsi_19 in ('T,F', 'F,T') or
       prsi_20 in ('T,F', 'F,T') or
       prsi_21 in ('T,F', 'F,T') or prsi_22 in ('T,F', 'F,T') or prsi_23 in ('T,F', 'F,T') or
       prsi_24 in ('T,F', 'F,T') or
       prsi_25 in ('T,F', 'F,T') or prsi_26 in ('T,F', 'F,T') or prsi_27 in ('T,F', 'F,T') or
       prsi_28 in ('T,F', 'F,T') or
       prsi_29 in ('T,F', 'F,T') or prsi_30 in ('T,F', 'F,T') or prsi_31 in ('T,F', 'F,T') or
       prsi_32 in ('T,F', 'F,T') or
       prsi_33 in ('T,F', 'F,T') or prsi_34 in ('T,F', 'F,T') or prsi_35 in ('T,F', 'F,T') or
       prsi_36 in ('T,F', 'F,T') or
       prsi_37 in ('T,F', 'F,T') or prsi_38 in ('T,F', 'F,T') or prsi_39 in ('T,F', 'F,T') or
       prsi_40 in ('T,F', 'F,T') or
       prsi_41 in ('T,F', 'F,T') or prsi_42 in ('T,F', 'F,T') or prsi_43 in ('T,F', 'F,T') or
       prsi_44 in ('T,F', 'F,T') or
       prsi_45 in ('T,F', 'F,T') or prsi_46 in ('T,F', 'F,T') or prsi_47 in ('T,F', 'F,T') or
       prsi_48 in ('T,F', 'F,T') or
       prsi_49 in ('T,F', 'F,T') or prsi_50 in ('T,F', 'F,T'))

select *
from ref_d18_ppc
where gsp_group_id = '_A'
  and pcl_id = 1
  and pfl_id = 1
  and time_pattern_regime = '13016'
order by st_date

select distinct st_date
from ref_d18_ppc
where prsi_50 = 'T'

select *
from ref_d18_ppc
where time_pattern_regime = '13016'
order by st_date

select date_part(week, '20190830')::varchar(2) + '-' + date_part(day, '2019-08-30')::char

select * from temp_avg_ppc where st_week_day = '2-2' and time_pattern_regime = '13016'
select * from ref_d18_ppc where time_pattern_regime = '13016' and prsi_12 = 'F' and date_part(weekday,to_date(st_date,'YYYYMMDD')) = 0

select * from ref_registers where registers_tpr = '13016'
select * from ref_registers where account_id = 27197