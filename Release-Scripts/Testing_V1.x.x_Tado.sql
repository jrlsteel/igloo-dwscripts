/*
 1) Check the comparison sql code returns nothing when comparing a table to itself
    (by definition, no changes should be found)
 */
select * from temp_tado_diffs_identical;
select * from temp_tado_audit_identical;

