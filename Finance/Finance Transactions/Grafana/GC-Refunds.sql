; with cte_refunds_gcrefunds as (
       select
         *
        from public.vw_fin_trans_refunds ref
       where ref.created_at between '$StartDate' and '$EndDate'

      )

,  cte_payments_gcrefunds as (
       select
        *
        from public.vw_fin_trans_payments py
       where py.created_at between '$StartDate' and '$EndDate'

      )

, cte_ensek_gcrefunds as (
        select *
        from public.vw_fin_trans_ensek
      where (accountid is not null or createddate is not null or transamount is not null)
        and createddate between dateadd(day, 2, '$StartDate') and  dateadd(day, -2, '$EndDate')
      )

, cte_ensek_summ_gcrefunds as (
        select
        AccountId,
        CreatedDate,
        TransAmount,
        count(*) as Countif
        from cte_ensek_gcrefunds
       WHERE CreatedDate between '$StartDate' and '$EndDate'
       group by
        AccountId,
        CreatedDate,
        TransAmount
      )


, cte_gc_refunds_gcrefunds as (
select
 gc.customers_id
, gc.ensekaccountid
, gc.created_at
, gc.arrival_date
, gc.status
, gc.refund_amount
, ensek.Countif
from cte_refunds_gcrefunds gc
left join cte_ensek_summ_gcrefunds ensek
    on gc.ensekAccountId = ensek.AccountId and
       gc.refund_amount = ensek.TransAmount
  )

select * from cte_gc_refunds_gcrefunds order by 1
;