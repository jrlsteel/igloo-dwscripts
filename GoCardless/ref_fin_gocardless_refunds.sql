select nullif(ensekid, '')::text,
       amount::bigint,
       nullif(created_at, '')::timestamp,
       nullif(currency, '')::text,
       nullif(id, '')::text,
       nullif(mandate, '')::text,
       nullif(metadata, '')::text,
       nullif(payment, '')::text,
       nullif(reference, '')::text,
       nullif(status, '')::text,
       nullif(extract_timestamp, '')::timestamp as last_updated_igloo,
       getdate()                                as etlchange
from aws_fin_stage1_extracts.fin_stage2_gocardlessrefunds