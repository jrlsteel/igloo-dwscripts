-- Read Only User  Creation
--Run in  igloosense-uat schema 
create user igloo_dw_uat_user password '**************';
grant usage on schema public to igloo_dw_uat_user;
grant usage on schema aws_s3_stage1_extracts to igloo_dw_uat_user;
grant usage on schema aws_s3_stage2_extracts to igloo_dw_uat_user;


alter user igloo_dw_uat_user set search_path to public;
grant select on all tables in schema public to igloo_dw_uat_user;
grant select on all tables in schema aws_s3_stage1_extracts to igloo_dw_uat_user;
grant select on all tables in schema aws_s3_stage2_extracts to igloo_dw_uat_user;



-- Prod Read Only User
--Run in  igloosense-prod schema 
create user igloo_dw_prod_user password '**************';
grant usage on schema public to igloo_dw_prod_user;
grant usage on schema aws_s3_stage1_extracts to igloo_dw_prod_user;
grant usage on schema aws_s3_stage2_extracts to igloo_dw_prod_user;


alter user igloo_dw_prod_user set search_path to public;
grant select on all tables in schema public to igloo_dw_prod_user;
grant select on all tables in schema aws_s3_stage1_extracts to igloo_dw_prod_user;
grant select on all tables in schema aws_s3_stage2_extracts to igloo_dw_prod_user;


alter group read_only_users set search_path to public
grant usage on schema public to read_only_users



-- FOR GROUPS
GRANT USAGE ON SCHEMA public TO GROUP read_only_users;
GRANT USAGE ON SCHEMA aws_s3_stage1_extracts TO GROUP read_only_users;
GRANT USAGE ON SCHEMA aws_s3_stage2_extracts TO GROUP read_only_users;
GRANT SELECT ON ALL TABLES IN SCHEMA  public TO GROUP read_only_users;
GRANT SELECT ON ALL TABLES IN SCHEMA  aws_s3_stage1_extracts TO GROUP read_only_users;
GRANT SELECT ON ALL TABLES IN SCHEMA  aws_s3_stage2_extracts TO GROUP read_only_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA  public GRANT SELECT ON TABLES TO GROUP read_only_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA  aws_s3_stage1_extracts GRANT SELECT ON TABLES TO GROUP read_only_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA  aws_s3_stage2_extracts GRANT SELECT ON TABLES TO GROUP read_only_users;
