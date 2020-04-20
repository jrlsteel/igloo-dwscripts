; with cte_refunds_go as (
       select
         *
        from public.vw_fin_trans_refunds ref
       where ref.created_at between '$StartDate' and '$EndDate'

      )

,  cte_payments_go as (
       select
        *
        from public.vw_fin_trans_payments py
       where py.created_at between '$StartDate' and '$EndDate'

      )

, cte_ensek_go as (
        select *
        from public.vw_fin_trans_ensek
      where (accountid is not null or createddate is not null or transamount is not null)
        and createddate between dateadd(day, 2, '$StartDate') and  dateadd(day, -2, '$EndDate')
      )

,  cte_payments_summ_go as (
       select
        ensekAccountId,
        created_at,
        "payments.amount",
        count(*) as Countif
        from cte_payments_go
       where (ensekAccountId is not null)
       and created_at between dateadd(day, 2, '$StartDate') and  dateadd(day, -2, '$EndDate')
       group by
        ensekAccountId,
        created_at ,
        "payments.amount"
      )

,   cte_refunds_summ_go as (
       select ref.ensekaccountid
        , ref.created_at
        , ref.refund_amount
        , count(*) as Countif
        from cte_refunds_go ref
       WHERE (ref.ensekaccountid is not null)
       and ref.created_at between dateadd(day, 2, '$StartDate') and  dateadd(day, -2, '$EndDate')
       group by
          ref.ensekaccountid
        , ref.created_at
        , ref.refund_amount
      )


, cte_ensek_summ_go as (
        select
        AccountId,
        CreatedDate,
        TransAmount,
        count(*) as Countif
        from cte_ensek_go
       group by
        AccountId,
        CreatedDate,
        TransAmount
      )


,   cte_ensek_tab_go as (
      select
       ensek.AccountId ,
       ensek.CreatedDate,
       ensek.TransAmount,
       ref.refund_amount ,
       paym."payments.amount" ,
       paym.Countif as C_Pay,
       ref.Countif as C_Ref
      from cte_ensek_go ensek
      left join cte_refunds_summ_go ref
          on ref.ensekAccountId = ensek.AccountId and
             ref.refund_amount = ensek.TransAmount
      left join cte_payments_summ_go paym
          on paym.ensekAccountId = ensek.AccountId and
             paym."payments.amount" = ensek.TransAmount and
             ensek.CreatedDate between dateadd(day, -3, paym.created_at) and dateadd(day, 3, paym.created_at) --- -3 0r +3 days ---
      WHERE ensek.CreatedDate between dateadd(day, 2, '$StartDate') and  dateadd(day, -2, '$EndDate')
 )


, cte_payments_tab_go as (
    select
     gc.ensekAccountId ,
     gc.created_at,
     gc."payments.amount",
     --ensek.TransAmount,
     ensek.Countif
    from cte_payments_go gc
    left join cte_ensek_summ_go ensek
        on gc.ensekAccountId = ensek.AccountId and
           gc."payments.amount" = ensek.TransAmount and
           --gc.created_at = ensek.CreatedDate
           gc.created_at between dateadd(day, -3, ensek.createdDate) and dateadd(day, 3, ensek.createdDate) --- -3 0r +3 days ---
    WHERE gc.created_at between dateadd(day, 2, '$StartDate') and  dateadd(day, -2, '$EndDate')
    )

, cte_ensek_debit_credit_go as (
--- Ensek Tab ---
select
 ensek.CreatedDate,
  SUM(CASE WHEN TransAmount::float > 0 THEN TransAmount::float ELSE null END) as Debits,
  SUM(CASE WHEN TransAmount::float < 0 THEN TransAmount::float ELSE null END) as Credits
from  cte_ensek_go ensek
Group  By ensek.CreatedDate
)


, cte_date_go as (
       select distinct CreatedDate as date_datetime
          from aws_fin_stage1_extracts.fin_sales_ledger_all_time
          where  CreatedDate between '$StartDate' and '$EndDate'
      )

, cte_ensek_report_go as (
select cd.date_datetime as ensek_date, ensek.Debits, ensek.Credits
 from cte_date_go cd
        left join cte_ensek_debit_credit_go ensek
        on ensek.CreatedDate = cd.date_datetime
      )


, cte_ensek_debit_credit_report as (
    select
    ensek_date,
    Debits,
    Credits,
    0 as rnk
    from cte_ensek_report_go

    UNION

    select
    'Total:' as ensek_date,
    sum(Debits) as Debits,
    sum(Credits) as Credits,
    1 as rnk
    from cte_ensek_report_go

    UNION

    select
    'Net Total:' as ensek_date,
    null           as Debits,
    sum(Debits) + sum(Credits) as Credits,
    2 as rnk
    from cte_ensek_report_go
)

select
    ensek_date,
    Debits,
    Credits
from cte_ensek_debit_credit_report
order by rnk,ensek_date ;