select ensekid::text,
       amount::bigint,
       created_at::timestamp,
       currency::text,
       id::text,
       mandate::text,
       metadata::text,
       payment::text,
       reference::text,
       status::text,
       extract_timestamp::timestamp as last_updated_igloo,
       getdate()                    as etlchange
from aws_fin_stage1_extracts.fin_stage2_gocardlessrefunds