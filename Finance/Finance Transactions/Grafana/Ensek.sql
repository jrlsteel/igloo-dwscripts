; with cte_gc as (
--- GC Tab ---
select
 gc.ensekAccountId ,
 gc.created_at,
 gc.payments_amount::float
from
       (
        select
        "payments.metadata.accountid"  as ensekAccountId,
        substring(created_at, 1, 10)::date as created_at,
        "payments.description" as payments_amount,
        "customers.given_name"  as "customers.id"
        from aws_fin_stage1_extracts.fin_go_cardless_payments
        )gc )



, cte_ensek as (
--- Ensek Tab ---
select
 ensek.AccountId ,
 ensek.CreatedDate,
 ensek.TransAmount::float,
  CASE WHEN TransAmount::float > '0' THEN TransAmount::float ELSE null END as Debits,
  CASE WHEN TransAmount::float < '0' THEN TransAmount::float ELSE null END as Credits
from  (
            select
                replace(accountid, ' ', '') as AccountId,
                substring(replace(createddate, ' ', ''), 1, 10)::date as CreatedDate,
                TransAmount -- replace(transamount, ' ', '') as TransAmount
                from aws_fin_stage1_extracts.fin_sales_ledger_journals
            where --lower(TransactionTypeName) = 'payment' and
            lower(AccountDesc) = 'card provider cash'
          ) ensek )


, cte_ensek_debits as (
  SELECT
  CreatedDate,
  SUM(Debits::float) as Debits
  from
     cte_ensek
  --where TransAmount::float > 0
  GROUP BY CreatedDate
)

, cte_ensek_credits as (
  SELECT
  CreatedDate,
  SUM(Credits::float) as Credits
  from
     cte_ensek
  --where TransAmount::float < 0
  GROUP BY CreatedDate
)


, c_rec as (
    SELECT ROW_NUMBER() OVER (ORDER BY c.accountid)
    from aws_fin_stage1_extracts.fin_sales_ledger_journals c
    limit 1000
)

,  cte_date as (SELECT substring(DATEADD(DAY, nbr - 1, '2020-01-01'), 1, 10) as date_datetime
                FROM (SELECT ROW_NUMBER() OVER (ORDER BY c.row_number) AS Nbr FROM c_rec c) nbrs
                WHERE nbr - 1 <= DATEDIFF(DAY, '2019-12-01', '2021-03-01'))


, cte_ensek_report as (
select cd.date_datetime as ensek_date, deb.Debits, cre.Credits
 from cte_date cd
        left join cte_ensek_debits deb on deb.CreatedDate = cd.date_datetime
        left join cte_ensek_credits cre on cre.CreatedDate = cd.date_datetime
where cd.date_datetime between '$StartDate' and '$EndDate'
      )


select * from cte_ensek_report order by 1;
