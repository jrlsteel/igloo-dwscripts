; with cte_gc as (
--- GC Tab ---
select
 gc.ensekAccountId ,
 gc.created_at,
 gc.payments_amount::float,
 SUM(CASE WHEN gc.ensekAccountId = ensek.AccountId and gc.ensekAccountId is not null
          THEN 1
          ELSE 0
      END ) as Countif
from
       (
        select
        "payments.metadata.accountid"  as ensekAccountId,
        substring(created_at, 1, 10)::date as created_at,
        "payments.description" as payments_amount,
        "customers.given_name"  as "customers.id"
        from aws_fin_stage1_extracts.fin_go_cardless_payments
        )gc
left join (
            select
                replace(accountid, ' ', '') as AccountId,
                substring(replace(createddate, ' ', ''), 1, 10)::date as CreatedDate,
                replace(transamount, ' ', '') as TransAmount
                from aws_fin_stage1_extracts.fin_sales_ledger_journals
             where accountid is not null or createddate is not null or transamount is not null
             -- and lower(TransactionTypeName) = 'payment'
             and lower(AccountDesc) = 'card provider cash'
          ) ensek
    on gc.ensekAccountId = ensek.AccountId and
       gc.payments_amount = ensek.TransAmount and
       gc.created_at between dateadd(day, -3, ensek.CreatedDate) and dateadd(day, 3, ensek.CreatedDate) --- -3 0r +3 days ---
Group BY
   gc.ensekAccountId ,
   gc.created_at,
   gc.payments_amount )



, cte_ensek as (
--- Ensek Tab ---
select
 ensek.AccountId ,
 ensek.CreatedDate,
 ensek.TransAmount::float,
 SUM(CASE WHEN gc.ensekAccountId = ensek.AccountId and gc.ensekAccountId is not null
          THEN 1
          ELSE 0
      END ) as Countif
from  (
            select
                replace(accountid, ' ', '') as AccountId,
                substring(replace(createddate, ' ', ''), 1, 10)::date as CreatedDate,
                replace(transamount, ' ', '') as TransAmount
                from aws_fin_stage1_extracts.fin_sales_ledger_journals
                where --lower(TransactionTypeName) = 'payment' and
                lower(AccountDesc) = 'card provider cash'
          ) ensek
left join
         (
          select
          "payments.metadata.accountid"  as ensekAccountId,
          substring(created_at, 1, 10)::date as created_at,
          "payments.description" as payments_amount,
          "customers.given_name"  as "customers.id"
          from aws_fin_stage1_extracts.fin_go_cardless_payments
         where ("payments.metadata.accountid" is not null ) or
               (created_at is not null  or  created_at != '')or
               ("customers.given_name" is not null or "customers.given_name" != '')
          )gc
    on gc.ensekAccountId = ensek.AccountId and
       gc.payments_amount = ensek.TransAmount and
       ensek.CreatedDate between dateadd(day, -3, gc.created_at) and dateadd(day, 3, gc.created_at) --- -3 0r +3 days ---
Group BY
   ensek.AccountId ,
   ensek.CreatedDate,
   ensek.TransAmount )



, c_rec as (
    SELECT ROW_NUMBER() OVER (ORDER BY c.accountid)
    from aws_fin_stage1_extracts.fin_sales_ledger_journals c
    limit 1000
)

,  cte_date as (SELECT substring(DATEADD(DAY, nbr - 1, '2020-01-01'), 1, 10) as date_datetime
                FROM (SELECT ROW_NUMBER() OVER (ORDER BY c.row_number) AS Nbr FROM c_rec c) nbrs
                WHERE nbr - 1 <= DATEDIFF(DAY, '2019-12-01', '2021-03-01'))



select
cd.date_datetime as gc_date,
pay.Payments
from
cte_date cd
left join
    (
      select
      gc.created_at,
      sum(gc.payments_amount::float) as Payments
      from
      cte_gc gc
      group by
      gc.created_at
    ) pay
 on pay.created_at = cd.date_datetime
where cd.date_datetime between '$StartDate' and '$EndDate'
;