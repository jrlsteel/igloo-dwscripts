select mandate_id::text,
       customerid::text,
       new_mandate_id::text,
       created_at::timestamp,
       next_possible_charge_date::timestamp,
       case lower(payments_require_approval)
           when 'false' then False
           when 'true' then True
           else null end::boolean   as payments_require_approval,
       reference::text,
       scheme::text,
       status::text,
       creditor::text,
       customer_bank_account::text,
       ensekid::bigint,
       ensekstatementid::bigint,
       extract_timestamp::timestamp as last_updated_igloo,
       getdate()                    as etlchange
from aws_fin_stage1_extracts.fin_stage2_gocardlessmandates