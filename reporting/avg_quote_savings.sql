-- find the average savings over a set time period from quotes

select avg(projected_savings) as avg_saving, count(*) as num_quotes, count(projected_savings) as num_quotes_with_ps
from ref_cdb_quotes q
         inner join ref_cdb_registrations reg on reg.quote_id = q.id
         inner join ref_cdb_supply_contracts sc on sc.registration_id = reg.id
where sc.created_at between dateadd(months, -3, current_date) and current_date
-- and projected_savings is not null

select count(*)                 as num_accounts,
       count(projected_savings) as num_quotes_with_ps,
       avg(projected_savings)   as avg_saving,
       min(signup_date)         as date_range_start,
       max(signup_date)         as date_range_end
from ref_calculated_daily_customer_file
where signup_date between dateadd(months, -3, getdate()) and getdate()
  and account_status != 'Cancelled'


select count(*)                  num_signups_nc,
       count(projected_savings)  num_proj_sav,
       sum(projected_savings) as sum_proj_sav,
       trunc(signup_date)     as signup_date
from ref_calculated_daily_customer_file
where account_status != 'Cancelled'
group by trunc(signup_date)
order by signup_date