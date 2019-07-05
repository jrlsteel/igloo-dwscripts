create table ref_calculated_tado_efficiency_average
(
	avg_perc_diff double precision,
	avg_savings_in_pounds double precision,
	stdev_perc_diff double precision,
	stdev_savings_in_pounds double precision,
	etlchange timestamp
)
;

alter table ref_calculated_tado_efficiency_average owner to igloo
;

create table ref_calculated_tado_efficiency_batch
(
	user_id bigint,
	account_id bigint distkey,
	supply_address_id bigint,
	postcode varchar(255),
	fuel_type varchar(65535),
	base_temp double precision,
	heating_basis varchar(255),
	heating_control_type varchar(255),
	heating_source varchar(255),
	house_bedrooms varchar(255),
	house_type varchar(255),
	house_age varchar(255),
	ages varchar(65535),
	family_category varchar(500),
	mmh_tado_status varchar(255),
	base_temp_used double precision,
	estimated_temp double precision,
	base_hours double precision,
	estimated_hours double precision,
	base_mit double precision,
	estimated_mit double precision,
	mmhkw_heating_source varchar(255),
	mmhkw_floor_area_band integer,
	mmhkw_prop_age_id integer,
	mmhkw_prop_type_id integer,
	region_id integer,
	annual_consumption double precision,
	annual_consumption_source varchar(255),
	unit_rate_with_vat double precision,
	unit_rate_source varchar(255),
	savings_perc double precision,
	avg_savings_perc double precision,
	savings_perc_source varchar(255),
	amount_over_year double precision,
	savings_in_pounds double precision,
	segment integer,
	etlchange timestamp
)
diststyle key
;

alter table ref_calculated_tado_efficiency_batch owner to igloo
;

