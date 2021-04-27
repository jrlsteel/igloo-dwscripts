with cte_data as (
    select * from ref_alp_igloo_cv
)
select count(*) over() as count,
       sum(value) over() as last_numerical_sum,
       data.*
from cte_data data