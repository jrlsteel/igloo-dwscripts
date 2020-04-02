; with cte_payments as (

       select
        ensekAccountId  as ensekAccountId,
        to_date(substring(charge_date, 1, 10), 'YYYY-MM_DD') as created_at,
        trunc(amount::float, 2) as "payments.amount",
        customers_id  as "customers.id"
        from public.vw_fin_go_cardless_api_payments
      )

, cte_ensek as (
        select
        replace(accountid, ' ', '') as AccountId,
        to_date(substring(replace(createddate, ' ', ''), 1, 10), 'YYYY-MM-DD') as CreatedDate,
        trunc(replace(transamount, ' ', '')::float, 2) as TransAmount
        from aws_fin_stage1_extracts.fin_sales_ledger_all_time
         where accountid is not null or createddate is not null or transamount is not null
         --and lower(TransactionTypeName) = 'payment'
        and lower(AccountDesc) = 'card provider cash'

      )

, cte_ensek_summ as (
        select
        AccountId,
        CreatedDate,
        TransAmount,
        count(*) as Countif
        from cte_ensek
       group by
        AccountId,
        CreatedDate,
        TransAmount
      )



select
 gc.ensekAccountId ,
 gc.created_at,
 gc."payments.amount",
 --ensek.TransAmount,
 ensek.Countif
from cte_payments gc
left join cte_ensek_summ ensek
    on gc.ensekAccountId = ensek.AccountId and
       gc."payments.amount" = ensek.TransAmount and
       --gc.created_at = ensek.CreatedDate
       gc.created_at between dateadd(day, -3, ensek.CreatedDate) and dateadd(day, 3, ensek.CreatedDate) --- -3 0r +3 days ---
WHERE gc.created_at between '$StartDate' and '$EndDate'
;
