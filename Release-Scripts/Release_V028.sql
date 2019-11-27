-- 1)
create table ref_calculated_tado_efficiency_batch_191127 as select * from ref_calculated_tado_efficiency_batch;

-- 2)
-- auto-generated definition
create table ref_compare_sql_config
(
    old_table   varchar(100),
    new_table   varchar(100),
    key_cols    varchar(500),
    destination varchar(100)
);

alter table ref_compare_sql_config
    owner to igloo;

