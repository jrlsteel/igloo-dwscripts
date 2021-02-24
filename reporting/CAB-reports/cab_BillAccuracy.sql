
select
--        y.account_id,
--        y.meter_point_id,
--        y.startdate,
--        y.enddate,
--        y.days_active,
       y.meterpointtype,
--        y.has_customer_valid_read,
--        y.has_bill_transactiontype
       count(distinct y.account_id) as disctinct_accounts,
       count(distinct y.meter_point_id) as distinct_meterpoints
       from (
select
       x.account_id,
       x.meterpointtype,
       x.meter_point_id,
       x.startdate,
       x.enddate,
       datediff(days, x.startdate, enddate) days_active,
       reading_datetime,
       transaction_type,
       transaction_date,
       max(case when x.reading_datetime is not null then 1 else 0 end) over (partition by account_id, meterpointtype) as has_customer_valid_read,
       max(case when x.transaction_type is not null then 1 else 0 end) over (partition by account_id)as has_bill_transactiontype

          from
              (select su.external_id as account_id,
                      mp.meter_point_id,
                      r.register_id,
                       mp.meterpointtype,
                       greatest(mp.supplystartdate,mp.associationstartdate) as startdate,
                       least(mp.supplyenddate, mp.associationenddate) as enddate,
                       dateadd(months, 12, greatest(mp.supplystartdate,mp.associationstartdate)) startdate_plus_12m,
                      ri.meterreadingdatetime as reading_datetime,
                      trunc(cast(creationdetail_createddate as timestamp)) transaction_date,
                      transactiontype as transaction_type
                from ref_cdb_supply_contracts su
                       inner join ref_meterpoints mp on su.external_id = mp.account_id
                       inner join ref_meters m on m.account_id = su.external_id and m.meter_point_id = mp.meter_point_id and m.removeddate is null
                       inner join ref_registers r on r.account_id = su.external_id and r.meter_id = m.meter_id
                       left outer join ref_readings_internal_valid ri on r.account_id = su.external_id
                                                                and ri.register_id = r.register_id and
                                                                    (
                                                                    (ri.meterreadingsourceuid = 'DC' and mp.supplystartdate != ri.meterreadingdatetime)
                                                                    or
                                                                    ri.meterreadingsourceuid = 'CUSTOMER'
                                                                    )
                       left outer join aws_s3_stage2_extracts.stage2_accounttransactions at on
                        at.account_id = su.external_id and
                         trunc(cast(creationdetail_createddate as timestamp)) >= ri.meterreadingdatetime and
                        trunc(cast(creationdetail_createddate as timestamp)) <= '2019-06-30' and
                        transactiontype = 'BILL'
              ) x
              where x.account_id is not null
                  and startdate_plus_12m <= '2019-06-30' --started supply at least before 12 months $(end_of_quarter_date)
                  and (
                      enddate is null -- still on supply
                        or
                      (enddate is not null and enddate >= '2019-06-30') -- ended supply after EOQ
                      )
             order by x.account_id
             ) y
             where y.has_customer_valid_read = 1 and y.has_bill_transactiontype = 1

  group by
--        y.account_id,
--        y.meter_point_id,
--        y.startdate,
--        y.enddate,
--        y.days_active,
       y.meterpointtype
--       y.has_customer_valid_read,
--        y.has_bill_transactiontype
;


select
       y.account_id,
       y.meter_point_id,
       y.startdate,
       y.enddate,
       y.days_active,
       y.meterpointtype,
       y.has_customer_valid_read,
       y.has_bill_transactiontype
--        count(distinct y.account_id) as disctinct_accounts,
--        count(distinct y.meter_point_id) as distinct_meterpoints
       from (
select
       x.account_id,
       x.meterpointtype,
       x.meter_point_id,
       x.startdate,
       x.enddate,
       datediff(days, x.startdate, enddate) days_active,
       reading_datetime,
       transaction_type,
       transaction_date,
       max(case when x.reading_datetime is not null then 1 else 0 end) over (partition by account_id, meterpointtype) as has_customer_valid_read,
       max(case when x.transaction_type is not null then 1 else 0 end) over (partition by account_id)as has_bill_transactiontype

          from
              (select su.external_id as account_id,
                      mp.meter_point_id,
                      r.register_id,
                       mp.meterpointtype,
                       greatest(mp.supplystartdate,mp.associationstartdate) as startdate,
                       least(mp.supplyenddate, mp.associationenddate) as enddate,
                       dateadd(months, 12, greatest(mp.supplystartdate,mp.associationstartdate)) startdate_plus_12m,
                      ri.meterreadingdatetime as reading_datetime,
                      trunc(cast(creationdetail_createddate as timestamp)) transaction_date,
                      transactiontype as transaction_type
                from ref_cdb_supply_contracts su
                       inner join ref_meterpoints mp on su.external_id = mp.account_id
                       inner join ref_meters m on m.account_id = su.external_id and m.meter_point_id = mp.meter_point_id and m.removeddate is null
                       inner join ref_registers r on r.account_id = su.external_id and r.meter_id = m.meter_id
                       left outer join ref_readings_internal_valid ri on r.account_id = su.external_id
                                                                and ri.register_id = r.register_id and
                                                                    (
                                                                    (ri.meterreadingsourceuid = 'DC' and mp.supplystartdate != ri.meterreadingdatetime)
                                                                    or
                                                                    ri.meterreadingsourceuid = 'CUSTOMER'
                                                                    ) -- pick only cus
                       left outer join aws_s3_stage2_extracts.stage2_accounttransactions at on
                        at.account_id = su.external_id and
                         trunc(cast(creationdetail_createddate as timestamp)) >= ri.meterreadingdatetime and
                        trunc(cast(creationdetail_createddate as timestamp)) <= '2019-06-30' and
                        transactiontype = 'BILL'
              ) x
              where x.account_id is not null
                  and startdate_plus_12m <= '2019-06-30' --started supply at least before 12 months $(end_of_quarter_date)
                  and (
                      enddate is null -- still on supply
                        or
                      (enddate is not null and enddate >= '2019-06-30') -- ended supply after EOQ
                      )
             order by x.account_id
             ) y
--              where y.has_customer_valid_read = 1 and y.has_bill_transactiontype = 1

  group by
       y.account_id,
       y.meter_point_id,
       y.startdate,
       y.enddate,
       y.days_active,
       y.meterpointtype,
      y.has_customer_valid_read,
       y.has_bill_transactiontype
;

select * from aws_s3_stage2_extracts.stage2_accounttransactions at
where at.account_id = 14206 ;

select * from ref_readings_internal_valid where meterpointnumber = '1100003779370';
select * from aws_s3_stage2_extracts.stage2_accounttransactions at where
                        at.account_id = 6926 and
                         trunc(cast(creationdetail_createddate as timestamp)) >= '2019-06-19' and
                        trunc(cast(creationdetail_createddate as timestamp)) <= '2019-06-30' and
                        transactiontype = 'BILL'


select meterreadingsourceuid from ref_readings_internal_valid
group by meterreadingsourceuid;