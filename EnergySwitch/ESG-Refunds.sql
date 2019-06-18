/* Energy Switch Gaurantee - Refunds */
select
  ta.account_id as account_id,
  at2.creationdetail_createddate as transaction_date,
  at2.transactiontype as transaction_type,
  at2.transactiontypefriendlyname description,
  case when su.external_id is null then 'N' else 'Y' end account_id_found,
  case when at2.account_id is null then 'N' else 'Y' end last_transaction_with_refund_found
--   ac.status
from temp_account_id ta -- Insert the account_ids into this table and run this query
     left outer join ref_cdb_supply_contracts su on su.external_id = ta.account_id
     -- inner join ref_account_status ac on ac.account_id = su.external_id
     left outer join (select at.* from aws_s3_stage2_extracts.stage2_accounttransactions at
                        inner join (select cast(a.account_id as bigint),  max(cast(creationdetail_createddate as timestamp)) created_date
                                    from aws_s3_stage2_extracts.stage2_accounttransactions a
                                    group by a.account_id) at1
                        on cast(at.account_id as bigint) = at1.account_id and cast(at.creationdetail_createddate as timestamp) = at1.created_date
                      where at.transactiontype = 'R') at2
               on cast(at2.account_id as bigint) = ta.account_id

order by ta.account_id;

-- Temp table used to store account_ids to run the refunds sql.
-- delete from temp_account_id;

insert into temp_account_id values
(2536),
(3678),
(5113),
(6287),
(6810),
(6871),
(7379),
(7381),
(7957),
(8067),
(8156),
(8323),
(8479),
(8506),
(8513),
(11517),
(11687),
(12564),
(12976),
(14620),
(14829),
(14940),
(15197),
(15386),
(16140),
(16725),
(16872),
(19058),
(20067),
(20847),
(21398),
(21663),
(21918),
(22125),
(22294),
(22913),
(23139),
(23893),
(25201),
(25684),
(27596),
(27829),
(28585),
(30010),
(30252),
(30763),
(30798),
(31943),
(32214),
(34870),
(34974),
(35147),
(35841),
(36151),
(36157),
(36384),
(36785),
(37535),
(37587),
(39028),
(39105),
(41102),
(41869),
(42423),
(42539),
(42642),
(43112),
(43126),
(43131),
(43715),
(43867),
(48834)