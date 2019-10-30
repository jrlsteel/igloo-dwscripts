create table if not exists ref_calculated_tado_efficiency_average_audit
(
	avg_perc_diff double precision,
	avg_savings_in_pounds double precision,
	stdev_perc_diff double precision,
	stdev_savings_in_pounds double precision,
	etlchange timestamp
)
;

alter table ref_calculated_tado_efficiency_average_audit owner to igloo
;