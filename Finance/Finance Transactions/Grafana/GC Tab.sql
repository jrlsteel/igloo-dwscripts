select
 gc.ensekAccountId ,
 gc.created_at,
 gc."payments.amount",
 SUM(CASE WHEN gc.ensekAccountId = ensek.AccountId and gc.ensekAccountId is not null
          THEN 1
          ELSE 0
      END ) as Countif
from
       (
        select
        "payments.metadata.accountid"  as ensekAccountId,
        substring(created_at, 1, 10)::date as created_at,
        "payments.description" as "payments.amount",
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
             --and lower(TransactionTypeName) = 'payment'
            and lower(AccountDesc) = 'card provider cash'
          ) ensek
    on gc.ensekAccountId = ensek.AccountId and
       gc."payments.amount" = ensek.TransAmount and
       gc.created_at between dateadd(day, -3, ensek.CreatedDate) and dateadd(day, 3, ensek.CreatedDate) --- -3 0r +3 days ---
WHERE gc.created_at between '$StartDate' and '$EndDate'
Group BY
   gc.ensekAccountId ,
   gc.created_at,
   gc."payments.amount" ;