select x.account_id,
       x.balance,
       x.dd_1 as direct_debit,
       x.dd_2 as direct_debit_2
        from (select
                      cast(act.account_id as bigint),
                      act.creationdetail_createddate                                                      as transaction_date,
                      -(cast(act.currentbalance as double precision))                                     as balance,
                      abs(cast(sdd.amount as double precision))                                           as dd_1,
                      abs(cast(sdd.amount as double precision)) * 2                                       as dd_2,
                      dd.direct_debit,
--                       sdd.amount                                                                           as direct_debit,
--                       act.method                                                                           as payment_method,
--                       act.transactiontype                                                                  as transaction_type,
                      row_number() over (partition by act.account_id order by creationdetail_createddate desc) as row_number_1
               from aws_s3_stage2_extracts.stage2_accounttransactions act
                   left outer join (select * from (
                              select account_id, abs(cast(dd.amount as double precision)) as direct_debit,
                                     row_number() over (partition by dd.account_id order by creationdetail_createddate desc) as row_number
                              from aws_s3_stage2_extracts.stage2_accounttransactions dd
                              where dd.method = 'Direct Debit') dd1 -- infer dd from transactions table
                              where dd1.row_number = 1) dd on dd.account_id = act.account_id
                   left outer join aws_s3_stage2_extracts.stage2_directdebit sdd on sdd.account_id = act.account_id
           ) x
where x.row_number_1 = 1
and ((x.balance > 0 and x.balance > x.dd_2) or (x.balance < 0 and x.balance < -x.dd_1))
order by x.account_id;