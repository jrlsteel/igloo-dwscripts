create view vw_etl_smart_billing_reads as

  with cte_accountsettings as (
        select * from
           (
            SELECT t.accountid, t.nextbilldate,
                   row_number() over(partition by t.accountid order by t.nextbilldate desc) as RowID
            FROM aws_s3_stage2_extracts.stage2_accountsettings t
           where
               DATEDIFF(days,substring(getdate(), 1, 10)::timestamp,
                   substring(t.nextbilldate, 1, 10)::timestamp) between 0 and 5
          ) stg
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
                    from
                         public.ref_readings_smart_daily rrsd
                           left join public.ref_calculated_daily_customer_file dcf on rrsd.account_id = dcf.account_id
                           left join public.cte_accountsettings acc on rrsd.account_id = acc.accountid
                    --and rmp.account_id = 1831
                    order by 1, 2, 3)



, cte_qry2 as (
       select *,
       row_number() over(partition by account_id order by timestamp desc) as RowID
       from cte_qry1
      )

select
    deviceid as ManualMeterReadingId,
    account_id as accountID,
    timestamp  as meterReadingDateTime,
    type  as meterType,
    mpxn as meterPointNumber,
    meter_id as meter,
    register_id as register,
    total_consumption as reading ,
   'SMART' as source,
   NULL as createdBy,
  next_bill_date

from cte_qry2
where RowID = 1
    with no schema binding
;