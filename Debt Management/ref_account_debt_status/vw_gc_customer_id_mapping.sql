create or replace view public.vw_gocardless_customer_id_mapping as
select combined_mapping.gc_id as client_id, combined_mapping.ensek_id as igl_acc_id
from (select customerid as gc_id, accountid as ensek_id
      from aws_fin_stage1_extracts.fin_go_cardless_id_mandate_lookup
      union
      distinct
      select client_id as gc_id, nullif(ensekid, '')::bigint as ensek_id
      from aws_fin_stage1_extracts.fin_stage2_gocardlessclients) combined_mapping
         left join public.ref_calculated_daily_customer_file dcf on dcf.account_id = combined_mapping.ensek_id
where account_status != 'Cancelled' and ensek_id is not null
with no schema binding;