GRANT USAGE ON SCHEMA aws_met_stage1_extracts TO GROUP read_only_users;
GRANT SELECT ON ALL TABLES IN SCHEMA  aws_met_stage1_extracts TO GROUP read_only_users;
ALTER DEFAULT PRIVILEGES IN SCHEMA  aws_met_stage1_extracts GRANT SELECT ON TABLES TO GROUP read_only_users;

