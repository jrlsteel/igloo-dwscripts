
/******** ref_d18_igloo_ppc ************/

-- create table for ppc
create table ref_d18_igloo_ppc
(
	zpd varchar(10),
	st_date timestamp,
	st_code varchar(10),
	rt_code varchar(10),
	run_no varchar(255),
	gsp_group varchar(255),
	gsp varchar(10),
	gsp_group_id varchar(10),
	noon_temp_act double precision encode bytedict,
	noon_temp_eff double precision encode bytedict,
	time_sunset varchar(255),
	sunset_variable varchar(255),
	pcl varchar(10),
	pcl_id integer encode delta,
	pfl varchar(10),
	pfl_id integer encode delta,
	ssc varchar(10),
	ss_conf_id varchar(10) distkey,
	vmr varchar(10),
	time_pattern_regime varchar(255),
	ppc varchar(10),
	ppc_sum double precision,
	row_number bigint,
	etlchange timestamp
)
diststyle key
sortkey(st_date)
;
-- select for ppc
select * from (
SELECT zpd,
       st_date,
       st_code,
       rt_code,
       run_no,
       gsp_group,
       gsp,
       gsp_group_id,
       noon_temp_act,
       noon_temp_eff,
       time_sunset,
       sunset_variable,
       pcl,
       pcl_id,
       pfl,
       pfl_id,
       ssc,
       ss_conf_id,
       vmr,
       time_pattern_regime,
       ppc,
       ppc_sum,
       dense_rank() over(partition by gsp_group_id, ss_conf_id, pcl_id, pfl_id, st_date order by run_no desc) as row,
       getdate() as etlchange
FROM vw_ref_d18_ppc
       group by
       zpd,
       st_date,
       st_code,
       rt_code,
       run_no,
       gsp_group,
       gsp,
       gsp_group_id,
       noon_temp_act,
       noon_temp_eff,
       time_sunset,
       sunset_variable,
       pcl,
       pcl_id,
       pfl,
       pfl_id,
       ssc,
       ss_conf_id,
       vmr,
       time_pattern_regime,
       ppc,
       ppc_sum,
       getdate()) p
where p.ss_conf_id in
      (select distinct attributes_attributevalue from ref_meterpoints_attributes where attributes_attributename = 'SSC')
  and p.pcl_id in
      (select distinct attributes_attributevalue from ref_meterpoints_attributes where attributes_attributename = 'Profile Class')
  and p.row = 1;


/******** ref_d18_igloo_bpp ************/

-- create table bpp
create table ref_d18_igloo_bpp
(
	zpd varchar(10),
	st_date timestamp distkey,
	st_code varchar(10),
	rt_code varchar(10),
	run_no varchar(255),
	gsp_group varchar(255),
	gsp varchar(10),
	gsp_group_id varchar(10),
	noon_temp_act double precision,
	noon_temp_eff double precision,
	time_sunset varchar(255),
	sunset_variable varchar(255),
	pcl varchar(10),
	pcl_id integer,
	pfl varchar(10),
	pfl_id integer,
	bpp varchar(10),
	bpp_sum double precision,
	row bigint,
	etlchange timestamp
)
diststyle key
sortkey(st_date)
;

-- select for bpp
select * from (
SELECT zpd,
       st_date,
       st_code,
       rt_code,
       run_no,
       gsp_group,
       gsp,
       gsp_group_id,
       noon_temp_act,
       noon_temp_eff,
       time_sunset,
       sunset_variable,
       pcl,
       pcl_id,
       pfl,
       pfl_id,
       bpp,
       bpp_sum,
       dense_rank() over(partition by gsp_group_id, pcl_id, pfl_id, st_date order by run_no desc) as row,
       getdate() as etlchange
FROM vw_ref_d18_bpp
  group by
       zpd,
       st_date,
       st_code,
       rt_code,
       run_no,
       gsp_group,
       gsp,
       gsp_group_id,
       noon_temp_act,
       noon_temp_eff,
       time_sunset,
       sunset_variable,
       pcl,
       pcl_id,
       pfl,
       pfl_id,
       bpp,
       bpp_sum,
       getdate()
       ) b
where b.pcl_id in
      (select distinct attributes_attributevalue from ref_meterpoints_attributes where attributes_attributename = 'Profile Class')
      and b.row = 1
;


/** To validate ref_d18_igloo_ppc for duplicates, should return 0 **/
select st_date from (
select st_date,
gsp_group_id,
ss_conf_id,
pcl_id,
pfl_id,
time_pattern_regime,
run_no,
count(*)
from ref_d18_igloo_ppc
group by st_date,
gsp_group_id,
ss_conf_id,
pcl_id,
pfl_id,
time_pattern_regime,
run_no
having count(*) > 1)
group by st_date
;

/** To validate ref_d18_igloo_bpp for duplicates, should return 0 **/
select st_date from (
select st_date,
gsp_group_id,
pcl_id,
pfl_id,
run_no,
count(*)
from ref_d18_igloo_bpp
group by st_date,
gsp_group_id,
pcl_id,
pfl_id,
run_no
having count(*) > 1)
group by st_date
;



/** To validate a single date **/
select * from ref_d18_igloo_ppc where gsp_group_id = '_D' and ss_conf_id = '0393' and time_pattern_regime = '00001'
and st_date >= '2017-09-20' and st_date < '2019-03-15' and pcl_id in (1);

