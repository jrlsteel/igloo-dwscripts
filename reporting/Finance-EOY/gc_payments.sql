with cte_data as (
    select payments.*,
            map.igl_acc_id                                as derived_ensek_id
    from ref_fin_gocardless_payments payments
        inner join ref_fin_gocardless_mandates mandates
            on mandates.mandate_id = payments.mandate
        inner join vw_gocardless_customer_id_mapping map
            on mandates.customerid = map.client_id
    where payments.created_at >= date_trunc('year', SYSDATE) - INTERVAL '1 year'
)
select count(*) over() as count,
       sum(derived_ensek_id) over() as last_numerical_sum,
       data.*
from cte_data data