; with cte_refunds as (
       select
         ref.customers_id
        , ref.ensekaccountid
        , to_date(substring(ref.created_at, 1, 10), 'YYYY-MM-DD') as created_at
        , ref.arrival_date
        , ref.status
        , trunc((ref.refund_amount * -1)::float, 2) as refund_amount
        , trunc((ref.amount_refunded * -1)::float, 2) as amount_refunded
        from public.vw_fin_go_cardless_api_refunds ref
       WHERE to_date(substring(ref.created_at, 1, 10), 'YYYY-MM-DD') between '$StartDate' and '$EndDate'
      )

, cte_ensek as (
        select
        replace(accountid, ' ', '') as AccountId,
        to_date(substring(replace(createddate, ' ', ''), 1, 10), 'YYYY-MM-DD') as CreatedDate,
        trunc(replace(transamount, ' ', '')::float, 2)  as TransAmount
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
       WHERE CreatedDate between '$StartDate' and '$EndDate'
       group by
        AccountId,
        CreatedDate,
        TransAmount
      )



select
 gc.customers_id
, gc.ensekaccountid
, gc.created_at
, gc.arrival_date
, gc.status
, gc.refund_amount
, ensek.Countif
from cte_refunds gc
left join cte_ensek_summ ensek
    on gc.ensekAccountId = ensek.AccountId and
       gc.refund_amount = ensek.TransAmount
;