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
       where to_date(substring(replace(ref.created_at, ' ', ''), 1, 10), 'YYYY-MM-DD') between '$StartDate' and '$EndDate'

      )

,  cte_payments as (
       select
        ensekAccountId  as ensekAccountId,
        to_date(substring(charge_date, 1, 10), 'YYYY-MM_DD') as created_at,
        trunc(amount::float, 2) as "payments.amount",
        customers_id  as "customers.id"
        from public.vw_fin_go_cardless_api_payments
       where to_date(substring(replace(charge_date, ' ', ''), 1, 10), 'YYYY-MM-DD') between '$StartDate' and '$EndDate'

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
       --and to_date(substring(replace(createddate, ' ', ''), 1, 10), 'YYYY-MM-DD') between '2020-01-01' and '2020-02-01'
      )

,  cte_payments_summ as (
       select
        ensekAccountId,
        created_at,
        "payments.amount",
        count(*) as Countif
        from cte_payments
       where ensekAccountId is not null
       group by
        ensekAccountId,
        created_at,
        "payments.amount"
      )

,   cte_refunds_summ as (
       select ref.ensekaccountid
        , ref.created_at
        , ref.refund_amount
        , count(*) as Countif
        from cte_refunds ref
       WHERE ref.ensekaccountid is not null
       group by
          ref.ensekaccountid
        , ref.created_at
        , ref.refund_amount
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


,   cte_ensek_tab as (
      select
       ensek.AccountId ,
       ensek.CreatedDate,
       ensek.TransAmount,
       ref.refund_amount ,
       paym."payments.amount" ,
       paym.Countif as C_Pay,
       ref.Countif as C_Ref
      from cte_ensek ensek
      left join cte_refunds_summ ref
          on ref.ensekAccountId = ensek.AccountId and
             ref.refund_amount = ensek.TransAmount
      left join cte_payments_summ paym
          on paym.ensekAccountId = ensek.AccountId and
             paym."payments.amount" = ensek.TransAmount and
             ensek.CreatedDate between dateadd(day, -3, paym.created_at) and dateadd(day, 3, paym.created_at) --- -3 0r +3 days ---
      WHERE ensek.CreatedDate between '$StartDate' and '$EndDate'
 )


, cte_payments_tab as (
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
    )