select count(*)
FROM ref_account_status;

select count(*)
FROM ref_tariff_history;


SELECT s.account_id, s.status,h.*
FROM ref_account_status s
       inner join ref_tariff_history h on s.account_id = h.account_id
WHERE s.status ='Live'
AND   h.end_date is null;

SELECT s.account_id, s.status,h.*
FROM ref_account_status s
       inner join ref_tariff_history h on s.account_id = h.account_id
WHERE s.status ='Live'
AND   h.end_date is not null;


SELECT count(*)
FROM ref_account_status s
       inner join ref_tariff_history h on s.account_id = h.account_id
WHERE s.status ='Live'
AND   h.end_date is null;

SELECT count(*)
FROM ref_account_status s
       inner join ref_tariff_history h on s.account_id = h.account_id;


select external_id from ref_cdb_supply_contracts
where external_id not in (select account_id from ref_tariff_history);

select * from ref_tariff_history
where account_id = 22066

SELECT count(*)
FROM ref_account_status s
       inner join ref_tariff_history h on s.account_id = h.account_id
WHERE s.status ='Live'
AND   h.end_date is not null;
