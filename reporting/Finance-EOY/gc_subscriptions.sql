with cte_data as (
    select subscriptions.*,
            map.igl_acc_id                                as derived_ensek_id
    from ref_fin_gocardless_subscriptions subscriptions
        inner join ref_fin_gocardless_mandates mandates
            on mandates.mandate_id = subscriptions.mandate
        inner join vw_gocardless_customer_id_mapping map
            on mandates.customerid = map.client_id
)
select count(*) over() as count,
       sum(derived_ensek_id) over() as last_numerical_sum,
       data.*
from cte_data data






