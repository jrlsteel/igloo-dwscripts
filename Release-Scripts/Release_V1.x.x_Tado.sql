/*
 BEFORE RELEASE OF GLUE SCRIPT
 1) create table temp_tado_old as backup of ref_calulated_tado_efficiency_batch (transform null keys to -1)
 2) update table definition of ref_compare_sql_config
 3) put two rows in the config table - one for comparing identical tables (should return nothing) and one for comparing old to new
 4) make output tables - two audit style, two diff style

 AFTER RELEASE OF GLUE SCRIPT
 5) create table temp_tado_new as copy of ref_calculated_tado_efficiency_batch after new code had overwritten it
 */

-- 1)
drop table if exists temp_tado_old;
create table temp_tado_old as
select *
from ref_calculated_tado_efficiency_batch;
update temp_tado_old
set user_id = -1
where user_id is null;
update temp_tado_old
set account_id = -1
where account_id is null;
update temp_tado_old
set supply_address_id = -1
where supply_address_id is null;

-- 2)
drop table if exists ref_compare_sql_config;
create table ref_compare_sql_config
(
    old_table   varchar(100),
    new_table   varchar(100),
    key_cols    varchar(500),
    audit_table varchar(100),
    diffs_table varchar(100)
);
alter table ref_compare_sql_config
    owner to igloo;

-- 3)
insert into ref_compare_sql_config (old_table, new_table, key_cols, audit_table, diffs_table)
values ('temp_tado_old', 'temp_tado_new', 'user_id, account_id, supply_address_id', 'temp_tado_audit',
        'temp_tado_diffs'),
       ('temp_tado_old', 'temp_tado_old', 'user_id, account_id, supply_address_id', 'temp_tado_audit_identical',
        'temp_tado_diffs_identical');

-- 4)
-- Create audit table for the comparison sql script
drop table if exists temp_tado_audit;
create table temp_tado_audit
(
    user_id                   bigint,
    account_id                bigint,
    supply_address_id         bigint,
    postcode                  varchar(255),
    fuel_type                 varchar(65535),
    base_temp                 double precision,
    heating_basis             varchar(255),
    heating_control_type      varchar(255),
    heating_source            varchar(255),
    house_bedrooms            varchar(255),
    house_type                varchar(255),
    house_age                 varchar(255),
    ages                      varchar(65535),
    family_category           varchar(500),
    mmh_tado_status           varchar(10),
    base_temp_used            double precision,
    estimated_temp            double precision,
    base_hours                double precision,
    estimated_hours           double precision,
    base_mit                  double precision,
    estimated_mit             double precision,
    mmhkw_heating_source      varchar(256),
    mmhkw_floor_area_band     integer,
    mmhkw_prop_age_id         integer,
    mmhkw_prop_type_id        integer,
    region_id                 integer,
    annual_consumption        double precision,
    annual_consumption_source varchar(256),
    unit_rate_with_vat        double precision,
    unit_rate_source          varchar(11),
    savings_perc              double precision,
    avg_savings_perc          double precision,
    savings_perc_source       varchar(9),
    amount_over_year          double precision,
    savings_in_pounds         double precision,
    segment                   integer,
    etlchangetype             varchar(1),
    etlchange                 timestamp
);
alter table temp_tado_audit
    owner to igloo;

-- Create audit table for the identical-comparison sql script
drop table if exists temp_tado_audit_identical;
create table temp_tado_audit_identical
(
    user_id                   bigint,
    account_id                bigint,
    supply_address_id         bigint,
    postcode                  varchar(255),
    fuel_type                 varchar(65535),
    base_temp                 double precision,
    heating_basis             varchar(255),
    heating_control_type      varchar(255),
    heating_source            varchar(255),
    house_bedrooms            varchar(255),
    house_type                varchar(255),
    house_age                 varchar(255),
    ages                      varchar(65535),
    family_category           varchar(500),
    mmh_tado_status           varchar(10),
    base_temp_used            double precision,
    estimated_temp            double precision,
    base_hours                double precision,
    estimated_hours           double precision,
    base_mit                  double precision,
    estimated_mit             double precision,
    mmhkw_heating_source      varchar(256),
    mmhkw_floor_area_band     integer,
    mmhkw_prop_age_id         integer,
    mmhkw_prop_type_id        integer,
    region_id                 integer,
    annual_consumption        double precision,
    annual_consumption_source varchar(256),
    unit_rate_with_vat        double precision,
    unit_rate_source          varchar(11),
    savings_perc              double precision,
    avg_savings_perc          double precision,
    savings_perc_source       varchar(9),
    amount_over_year          double precision,
    savings_in_pounds         double precision,
    segment                   integer,
    etlchangetype             varchar(1),
    etlchange                 timestamp
);
alter table temp_tado_audit_identical
    owner to igloo;

-- Create diffs table for the comparison sql script
drop table if exists temp_tado_diffs;
create table temp_tado_diffs
(
    user_id           bigint,
    account_id        bigint,
    supply_address_id bigint,
    field             varchar(100),
    old_val           varchar(max),
    new_val           varchar(max),
    old_etlchange     timestamp,
    new_etlchange     timestamp
);
alter table temp_tado_diffs
    owner to igloo;

-- Create diffs table for the identical-comparison sql script
drop table if exists temp_tado_diffs_identical;
create table temp_tado_diffs_identical
(
    user_id           bigint,
    account_id        bigint,
    supply_address_id bigint,
    field             varchar(100),
    old_val           varchar(max),
    new_val           varchar(max),
    old_etlchange     timestamp,
    new_etlchange     timestamp
);
alter table temp_tado_diffs_identical
    owner to igloo;


-- 5)
drop table if exists temp_tado_new;
create table temp_tado_new as
select *
from ref_calculated_tado_efficiency_batch;
update temp_tado_new
set user_id = -1
where user_id is null;
update temp_tado_new
set account_id = -1
where account_id is null;
update temp_tado_new
set supply_address_id = -1
where supply_address_id is null;


/*create table temp_tado_20200117_morning as
select *
from temp_tado_old*/