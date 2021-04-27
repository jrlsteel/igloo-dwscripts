with cte_data as (
    select * from ref_tariffs
)
select count(*) over() as count,
       sum(nvl(exit_fees,0)) over() as last_numerical_sum,
       data.*
from cte_data data