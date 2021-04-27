with cte_data as (
    select * from ref_d18_igloo_ppc
    where st_date >= date_trunc('year', SYSDATE) - INTERVAL '1 year'
)
select count(*) over() as count,
       sum(ppc_sum) over() as last_numerical_sum,
       data.*
from cte_data data