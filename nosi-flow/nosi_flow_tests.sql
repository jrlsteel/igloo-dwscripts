select count(*) from aws_s3_stage1_extracts.stage1_readingsnosigas;
select count(*) from aws_s3_stage2_extracts.stage2_readingsnosigas;
select count(*) from ref_readings_internal_nosi;
select count(*) from ref_readings_internal;
select count(*) from vw_ref_readings_all;
select count(*) from ref_readings_internal_valid;
select count(*) from vw_ref_readings_all_valid;
select count(*) from ref_calculated_aq;
select count(*) from vw_ref_calculated_aq_all;
select count(*) from ref_calculated_aq;
select count(*) from vw_ref_calculated_aq_all;
select count(*) from ref_calculated_aq_v1;
select count(*) from vw_ref_calculated_aq_v1_all;

--Meterpoints fix going in this release.
select count(*) from ref_meterpoints;
select count(*) from ref_meterpoints_audit;


