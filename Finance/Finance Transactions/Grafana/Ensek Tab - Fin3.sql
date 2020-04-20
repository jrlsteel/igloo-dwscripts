; with cte_refunds_ensk as (
       select
         *
        from public.vw_fin_trans_refunds ref
       where ref.created_at between '$StartDate' and '$EndDate'

      )

,  cte_payments_ensk  as (
       select
        *
        from public.vw_fin_trans_payments py
       where py.created_at between '$StartDate' and '$EndDate'

      )

, cte_ensek_ensk  as (
        select *
        from public.vw_fin_trans_ensek
      where (accountid is not null or createddate is not null or transamount is not null)
        and createddate between dateadd(day, 2, '$StartDate') and  dateadd(day, -2, '$EndDate')
      )

,  cte_payments_summ_ensk  as (
       select
        ensekAccountId,
        created_at,
        "payments.amount",
        count(*) as Countif
        from cte_payments_ensk
       where ensekAccountId is not null
       group by
        ensekAccountId,
        created_at ,
        "payments.amount"
      )

,   cte_refunds_summ_ensk  as (
       select ref.ensekaccountid
        , ref.created_at
        , ref.refund_amount
        , count(*) as Countif
        from cte_refunds_ensk  ref
       WHERE ref.ensekaccountid is not null
       group by
          ref.ensekaccountid
        , ref.created_at
        , ref.refund_amount
      )

, cte_ensek_tab_ensk  as (
    select
     ensek.AccountId ,
     ensek.CreatedDate,
     ensek.TransAmount,
     ref.refund_amount ,
     paym."payments.amount" ,
     paym.Countif as C_Pay,
     ref.Countif as C_Ref
    from cte_ensek_ensk  ensek
    left join cte_refunds_summ_ensk  ref
        on ref.ensekAccountId = ensek.AccountId and
           ref.refund_amount = ensek.TransAmount
    left join cte_payments_summ_ensk  paym
        on paym.ensekAccountId = ensek.AccountId and
           paym."payments.amount" = ensek.TransAmount and
           ensek.CreatedDate between dateadd(day, -3, paym.created_at) and dateadd(day, 3, paym.created_at) --- -3 0r +3 days ---
    WHERE ensek.CreatedDate between '$StartDate' and '$EndDate'
    )

  select * from cte_ensek_tab_ensk  order by 1
  ;