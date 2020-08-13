create view vw_etl_account_ids_weekly as
(SELECT full_account_list.account_id
   FROM (((SELECT ref_meterpoints_raw.account_id FROM ref_meterpoints_raw
           UNION SELECT ref_occupier_accounts.account_id FROM ref_occupier_accounts)
          UNION SELECT ref_cdb_supply_contracts.external_id AS account_id
                FROM ref_cdb_supply_contracts) full_account_list LEFT JOIN (SELECT ref_meterpoints_raw.account_id,
                                                                                   "max"(
                                                                                     COALESCE(ref_meterpoints_raw.end_date, (getdate() + (1000) :: bigint))) AS sed
                                                                            FROM ref_meterpoints_raw
                                                                            GROUP BY ref_meterpoints_raw.account_id) acc_sed ON ((
     acc_sed.account_id = full_account_list.account_id)))
   WHERE (date_diff(('days' :: character varying) :: text, COALESCE(acc_sed.sed, (getdate() + (1000) :: bigint)),
                    getdate()) < 365)
   UNION SELECT ordered_transactions.account_id
         FROM (SELECT ref_account_transactions.account_id,
                      ref_account_transactions.creationdetail_createddate,
                      ref_account_transactions.currentbalance,
                      pg_catalog.row_number() OVER (PARTITION BY ref_account_transactions.account_id ORDER BY ref_account_transactions.creationdetail_createddate DESC) AS rn
               FROM ref_account_transactions) ordered_transactions
         WHERE ((ordered_transactions.rn = 1) AND ((ordered_transactions.currentbalance <> (0) :: double precision) OR
                                                   (date_diff(('days' :: character varying) :: text,
                                                              ordered_transactions.creationdetail_createddate,
                                                              getdate()) < 365))))
  UNION SELECT dwh_manual_batch_accounts.account_id
        FROM dwh_manual_batch_accounts
        WHERE ((getdate() >= COALESCE(dwh_manual_batch_accounts.use_from, (getdate() - (1) :: bigint))) AND
               (getdate() <= COALESCE(dwh_manual_batch_accounts.use_until, (getdate() + (1) :: bigint)))) ORDER BY 1
;

alter table vw_etl_account_ids_weekly owner to igloo
;

create view vw_etl_account_ids_daily as
(SELECT full_account_list.account_id
   FROM (((SELECT ref_meterpoints_raw.account_id FROM ref_meterpoints_raw
           UNION SELECT ref_occupier_accounts.account_id FROM ref_occupier_accounts)
          UNION SELECT ref_cdb_supply_contracts.external_id AS account_id
                FROM ref_cdb_supply_contracts) full_account_list LEFT JOIN (SELECT ref_meterpoints_raw.account_id,
                                                                                   "max"(
                                                                                     COALESCE(ref_meterpoints_raw.end_date, (getdate() + (1000) :: bigint))) AS sed
                                                                            FROM ref_meterpoints_raw
                                                                            GROUP BY ref_meterpoints_raw.account_id) acc_sed ON ((
     acc_sed.account_id = full_account_list.account_id)))
   WHERE (date_diff(('days' :: character varying) :: text, COALESCE(acc_sed.sed, (getdate() + (1000) :: bigint)),
                    getdate()) < 56)
   UNION SELECT ordered_transactions.account_id
         FROM (SELECT ref_account_transactions.account_id,
                      ref_account_transactions.creationdetail_createddate,
                      ref_account_transactions.currentbalance,
                      pg_catalog.row_number() OVER (PARTITION BY ref_account_transactions.account_id ORDER BY ref_account_transactions.creationdetail_createddate DESC) AS rn
               FROM ref_account_transactions) ordered_transactions
         WHERE ((ordered_transactions.rn = 1) AND ((ordered_transactions.currentbalance <> (0) :: double precision) OR
                                                   (date_diff(('days' :: character varying) :: text,
                                                              ordered_transactions.creationdetail_createddate,
                                                              getdate()) < 28))))
  UNION SELECT dwh_manual_batch_accounts.account_id
        FROM dwh_manual_batch_accounts
        WHERE (((getdate() >= COALESCE(dwh_manual_batch_accounts.use_from, (getdate() - (1) :: bigint))) AND
                (getdate() <= COALESCE(dwh_manual_batch_accounts.use_until, (getdate() + (1) :: bigint)))) AND
               dwh_manual_batch_accounts.daily_batch) ORDER BY 1
;

alter table vw_etl_account_ids_daily owner to igloo
;

create view vw_etl_acc_mp_ids as
SELECT DISTINCT ref_meterpoints_raw.account_id,
                  ref_meterpoints_raw.meter_point_id,
                  ref_meterpoints_raw.meterpointnumber,
                  ref_meterpoints_raw.meterpointtype
  FROM ref_meterpoints_raw
  WHERE (date_diff(('days' :: character varying) :: text,
                   GREATEST(ref_meterpoints_raw.supplystartdate, ref_meterpoints_raw.associationstartdate), getdate()) <
         7)
  ORDER BY ref_meterpoints_raw.account_id
;

alter table vw_etl_acc_mp_ids owner to igloo
;

create view vw_etl_pending_acc_ids_daily as
SELECT ref_meterpoints.account_id,
         min(GREATEST(ref_meterpoints.associationstartdate, ref_meterpoints.supplystartdate)) AS ssd
  FROM ref_meterpoints
  GROUP BY ref_meterpoints.account_id
  HAVING (date_diff(('days' :: character varying) :: text,
                    min(GREATEST(ref_meterpoints.associationstartdate, ref_meterpoints.supplystartdate)), getdate()) <=
          7)
;

alter table vw_etl_pending_acc_ids_daily owner to igloo
;

create view vw_etl_pending_acc_ids_weekly as
SELECT ref_meterpoints.account_id,
         min(GREATEST(ref_meterpoints.associationstartdate, ref_meterpoints.supplystartdate)) AS ssd
  FROM ref_meterpoints
  GROUP BY ref_meterpoints.account_id
  HAVING (date_diff(('days' :: character varying) :: text,
                    min(GREATEST(ref_meterpoints.associationstartdate, ref_meterpoints.supplystartdate)), getdate()) <=
          7)
;

alter table vw_etl_pending_acc_ids_weekly owner to igloo
;

create view vw_etl_epc_postcodes_daily as
SELECT (("left"((ref_cdb_addresses.postcode) :: text, (len((ref_cdb_addresses.postcode) :: text) - 3)) +
           (' ' :: character varying) :: text) + "right"((ref_cdb_addresses.postcode) :: text, 3)) AS postcode
  FROM ref_cdb_addresses
  GROUP BY (("left"((ref_cdb_addresses.postcode) :: text, (len((ref_cdb_addresses.postcode) :: text) - 3)) +
             (' ' :: character varying) :: text) + "right"((ref_cdb_addresses.postcode) :: text, 3))
;

alter table vw_etl_epc_postcodes_daily owner to igloo
;

create view vw_etl_epc_postcodes_weekly as
SELECT (("left"((ref_cdb_addresses.postcode) :: text, (len((ref_cdb_addresses.postcode) :: text) - 3)) +
           (' ' :: character varying) :: text) + "right"((ref_cdb_addresses.postcode) :: text, 3)) AS postcode
  FROM ref_cdb_addresses
  GROUP BY (("left"((ref_cdb_addresses.postcode) :: text, (len((ref_cdb_addresses.postcode) :: text) - 3)) +
             (' ' :: character varying) :: text) + "right"((ref_cdb_addresses.postcode) :: text, 3))
;

alter table vw_etl_epc_postcodes_weekly owner to igloo
;

create view vw_etl_land_regsitry_postcodes_weekly as
SELECT (("left"((ref_cdb_addresses.postcode) :: text, (len((ref_cdb_addresses.postcode) :: text) - 3)) +
           (' ' :: character varying) :: text) + "right"((ref_cdb_addresses.postcode) :: text, 3)) AS postcode
  FROM ref_cdb_addresses
  GROUP BY (("left"((ref_cdb_addresses.postcode) :: text, (len((ref_cdb_addresses.postcode) :: text) - 3)) +
             (' ' :: character varying) :: text) + "right"((ref_cdb_addresses.postcode) :: text, 3))
;

alter table vw_etl_land_regsitry_postcodes_weekly owner to igloo
;

create view vw_etl_land_registry_postcodes_daily as
SELECT addr.id,
         addr.sub_building_name_number,
         addr.building_name_number,
         addr.thoroughfare,
         addr.county,
         addr.postcode,
         addr.uprn
  FROM ((ref_cdb_addresses addr JOIN ref_cdb_supply_contracts sc ON ((addr.id =
                                                                      sc.supply_address_id))) JOIN (SELECT DISTINCT ref_meterpoints.account_id
                                                                                                    FROM ref_meterpoints) mp ON ((
    sc.external_id = mp.account_id)))
;

alter table vw_etl_land_registry_postcodes_daily owner to igloo
;

create view vw_etl_weather_postcodes_daily as
SELECT "left"((addr.postcode) :: text, (len((addr.postcode) :: text) - 3)) AS postcode
  FROM ((ref_cdb_addresses addr JOIN ref_cdb_supply_contracts sc ON ((addr.id =
                                                                      sc.supply_address_id))) JOIN (SELECT DISTINCT ref_meterpoints.account_id
                                                                                                    FROM ref_meterpoints) mp ON ((
    sc.external_id = mp.account_id)))
;

alter table vw_etl_weather_postcodes_daily owner to igloo
;

create view vw_etl_weather_postcodes_weekly as
SELECT "left"((addr.postcode) :: text, (len((addr.postcode) :: text) - 3)) AS postcode
  FROM ((ref_cdb_addresses addr JOIN ref_cdb_supply_contracts sc ON ((addr.id =
                                                                      sc.supply_address_id))) JOIN (SELECT DISTINCT ref_meterpoints.account_id
                                                                                                    FROM ref_meterpoints) mp ON ((
    sc.external_id = mp.account_id)))
;

alter table vw_etl_weather_postcodes_weekly owner to igloo
;

create view vw_etl_smart_billing_reads as

  with cte_accountsettings as (
      select *
      from (SELECT t.accountid,
                   t.nextbilldate,
                   row_number() over (partition by t.accountid order by t.nextbilldate desc) as RowID
            FROM aws_s3_stage2_extracts.stage2_accountsettings t
            where DATEDIFF(days, substring(getdate(), 1, 10) :: timestamp,
                           substring(t.nextbilldate, 1, 10) :: timestamp) between 0 and 5) stg
      where stg.RowID = 1
  )

    , cte_qry1 as (select rrsd.account_id,
                          rrsd.meterpoint_id,
                          rrsd.meter_id,
                          rrsd.register_id,
                          rrsd.mpxn,
                          rrsd.deviceid,
                          rrsd.total_consumption,
                          rrsd.type,
                          rrsd.register_num,
                          rrsd.register_value,
                          rrsd.timestamp,
                          dcf.supply_type,
                          acc.nextbilldate as next_bill_date
                   from public.ref_readings_smart_daily rrsd
                          left join public.ref_calculated_daily_customer_file dcf on rrsd.account_id = dcf.account_id
                          left join public.cte_accountsettings acc on rrsd.account_id = acc.accountid
      --and rmp.account_id = 1831
                   order by 1, 2, 3)


    , cte_qry2 as (
      select *, row_number() over (partition by account_id order by timestamp desc) as RowID
      from cte_qry1
  )

  select deviceid          as ManualMeterReadingId,
         account_id        as accountID,
         timestamp         as meterReadingDateTime,
         type              as meterType,
         mpxn              as meterPointNumber,
         meter_id          as meter,
         register_id       as register,
         total_consumption as reading,
         'SMART'           as source,
         NULL              as createdBy,
         next_bill_date

  from cte_qry2
  where RowID = 1
  with no schema binding
;

alter table vw_etl_smart_billing_reads owner to igloo
;

