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
                where
                -- lower(TransactionTypeName) = 'payment' and
                 lower(AccountDesc) = 'card provider cash'
          ) ensek
left join
         (
          select
          "payments.metadata.accountid"  as ensekAccountId,
          substring(created_at, 1, 10)::date as created_at,
          "payments.description" as "payments.amount",
          "customers.given_name"  as "customers.id"
          from aws_fin_stage1_extracts.fin_go_cardless_payments
         where ("payments.metadata.accountid" is not null ) or
               (created_at is not null  or  created_at != '')or
               ("customers.given_name" is not null or "customers.given_name" != '')
          )gc
    on gc.ensekAccountId = ensek.AccountId and
       gc."payments.amount" = ensek.TransAmount and
       ensek.CreatedDate between dateadd(day, -3, gc.created_at) and dateadd(day, 3, gc.created_at) --- -3 0r +3 days ---
WHERE ensek.CreatedDate between '$StartDate' and '$EndDate'
Group BY
   ensek.AccountId ,
   ensek.CreatedDate,
   ensek.TransAmount;