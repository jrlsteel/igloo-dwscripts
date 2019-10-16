-- 4 tables for redshift

create table ref_smart_meter_eligibility_zones
(
    zone_id    int distkey,
    area_name  varchar(30),
    zone_scope int,
    open_date  timestamp,
    etlchange  timestamp
)
    diststyle key
    sortkey (zone_id);

alter table ref_smart_meter_eligibility_zones
    owner to igloo;

create table ref_smart_meter_eligibility_postcode_mapping
(
    zone_id         int distkey,
    postcode_prefix varchar(4)
)
    diststyle key
    sortkey (zone_id, postcode_prefix);

alter table ref_smart_meter_eligibility_postcode_mapping
    owner to igloo;

create table ref_smart_meter_eligibility_postcode_overrides
(
    postcode_prefix varchar(4) distkey,
    override_start  timestamp,
    override_end    timestamp,
    reason          varchar(50)
)
    diststyle key
    sortkey (postcode_prefix, override_start);

alter table ref_smart_meter_eligibility_postcode_overrides
    owner to igloo;

create table ref_smart_meter_eligibility_accounts
(
    account_id         bigint distkey,
    eligibility_status varchar(10),
    detailed_status    varchar(30),
    eligible_from      timestamp,
    etlchange          timestamp
)
    diststyle key
    sortkey (account_id);

alter table ref_smart_meter_eligibility_accounts
    owner to igloo;

create table dwh_spark_logs
(
    job_id    integer                                       not null,
    priority  varchar(10) default 'info'::character varying not null,
    log_type  varchar(20),
    message   varchar(65535),
    etlchange timestamp
);

alter table dwh_spark_logs
    owner to igloo;




-- 1 table for Aurora

create table ref_smart_meter_eligibility_accounts
(
    account_id         bigint      null,
    eligibility_status varchar(10) null,
    detailed_status    varchar(30) null,
    eligible_from      timestamp   null,
    etlchange          timestamp   null
);

