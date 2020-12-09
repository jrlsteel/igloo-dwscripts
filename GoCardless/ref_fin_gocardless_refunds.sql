select nullif(ensekid, ''),
       nullif(amount, '')::bigint,
       nullif(created_at, '')::timestamp,
       nullif(currency, ''),
       nullif(id, ''),
       nullif(mandate, ''),
       nullif(metadata, ''),
       nullif(payment, ''),
       nullif(reference, ''),
       nullif(status, ''),
       nullif(extract_timestamp, '')::timestamp as last_updated_igloo,
       getdate()                                as etlchange
from aws_fin_stage1_extracts.fin_stage2_gocardlessrefunds