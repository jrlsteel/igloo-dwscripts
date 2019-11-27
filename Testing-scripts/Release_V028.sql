-- 2 ---  Copy results from ref_calculated_tado_efficiency_batch to temp_tado_new (overwrite) ---

TRUNCATE TABLE temp_tado_new;

INSERT INTO temp_tado_new
SELECT * FROM ref_calculated_tado_efficiency_batch;



-- 3 ---  Copy results from ref_calculated_tado_efficiency_batch_191127 to temp_tado_old (overwrite) ---

TRUNCATE TABLE temp_tado_old;

INSERT INTO temp_tado_old
SELECT * FROM ref_calculated_tado_efficiency_batch_191127;



-- 4 ---  Replace null values in key fields with -1 (a value no actual row will have) ---

--- temp_tado_new ---
UPDATE temp_tado_new t
set user_id = -1
where user_id is null;

UPDATE temp_tado_new t
set account_id = -1
where account_id is null;

UPDATE temp_tado_new t
set supply_address_id = -1
where supply_address_id is null;


--- temp_tado_old ---
UPDATE temp_tado_old t
set user_id = -1
where user_id is null;

UPDATE temp_tado_old t
set account_id = -1
where account_id is null;

UPDATE temp_tado_old t
set supply_address_id = -1
where supply_address_id is null;


-- 5 ---  Set up comparison SQL to compare temp_tado_old and temp_tado_new ---

truncate table ref_compare_sql_config;

INSERT INTO ref_compare_sql_config (old_table, new_table, key_cols, destination)
VALUES ('temp_tado_old', 'temp_tado_new', 'user_id, account_id, supply_address_id', 'temp_tado_diffs');
