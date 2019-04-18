drop table ref_calculated_tado_fuel;

create table ref_calculated_tado_fuel
(
	fuel_id bigint encode delta distkey,
	fuel_tariff_name varchar(255),
	fuel_start_date timestamp,
	fuel_end_date timestamp,
	fuel_description varchar(255),
	fuel_rate double precision,
	fuel_coeffeicient double precision,
	fuel_calculation varchar(255)
)
diststyle key
;

alter table ref_tariff_history_elec_ur owner to igloo
;

create table ref_cdb_mmh_need_postcode_lookup
(
	id bigint,
	outcode varchar(256),
	region_code varchar(256),
	region_id integer
);

alter table ref_cdb_mmh_need_postcode_lookup owner to igloo
;

insert into  ref_cdb_mmh_need_postcode_lookup
select * from aws_s3_stage2_extracts.stage2_cdbmmhneedpostcodelookup;


create table ref_cdb_mmh_need_profiling_lookup
(
	id bigint,
	region_id bigint,
	prop_type bigint,
	floor_area_band bigint,
	prop_age bigint,
	elec_usage_gas_heating bigint,
	elec_usage_elec_heating bigint,
	elec_usage_other_heating bigint,
	gas_usage_gas_heating bigint
)
;



alter table ref_cdb_mmh_need_profiling_lookup owner to igloo
;

insert into  ref_cdb_mmh_need_profiling_lookup
select * from aws_s3_stage2_extracts.stage2_cdbmmhneedprofilinglookup;
