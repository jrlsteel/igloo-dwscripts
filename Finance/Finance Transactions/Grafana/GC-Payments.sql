; with cte_refunds_gcpaym as (
       select
         *
        from public.vw_fin_trans_refunds ref
       where ref.created_at between '$StartDate' and '$EndDate'

      )

,  cte_payments_gcpaym as (
       select
        *
        from public.vw_fin_trans_payments py
       where py.created_at between '$StartDate' and '$EndDate'

      )

, cte_ensek_gcpaym as (
        select *
        from public.vw_fin_trans_ensek
      where (accountid is not null or createddate is not null or transamount is not null)
        and createddate between dateadd(day, 2, '$StartDate') and  dateadd(day, -2, '$EndDate')
      )

, cte_ensek_summ_gcpaym as (
        select
        AccountId,
        CreatedDate,
        TransAmount,
        count(*) as Countif
        from cte_ensek_gcpaym
       group by
        AccountId,
        CreatedDate,
        TransAmount
      )


, cte_gc_payments_gcpaym as (
select
 gc.ensekAccountId ,
 gc.created_at,
 gc."payments.amount",
 --ensek.TransAmount,
 ensek.Countif
from cte_payments_gcpaym gc
left join cte_ensek_summ_gcpaym ensek
    on gc.ensekAccountId = ensek.AccountId and
       gc."payments.amount" = ensek.TransAmount and
       --gc.created_at = ensek.CreatedDate
       ensek.CreatedDate between dateadd(day, -3, gc.created_at) and dateadd(day, 3, gc.created_at) --- -3 0r +3 days ---
WHERE gc.created_at between '$StartDate' and '$EndDate'
)


select * from cte_gc_payments_gcpaym order by 1
;
