SELECT id::text,
       created_at::timestamp,
       amount::bigint,
       currency::text,
       status::text,
       name::text,
       start_date::timestamp,
       end_date::timestamp,
       interval::bigint,
       interval_unit::text,
       replace(day_of_month, '.0', '')::bigint as day_of_month,
       month::int,
       count_no::float::bigint,
       payment_reference::text,
       replace(app_fee, '.0', '')::bigint      as app_fee,
       case lower(retry_if_possible)
           when 'false' then False
           when 'true' then True
           else null end::boolean              as retry_if_possible,
       mandate::text,
       charge_date::timestamp,
       amount_subscription::float::bigint,
       extract_timestamp::timestamp            as last_updated_igloo,
       getdate()                               as etlchange
FROM aws_fin_stage1_extracts.fin_stage2_gocardlesssubscriptions