create or replace view vw_etl_weather_forecast_hourly as
with cte_smart_meters as (
    /*
      Filter for only smart meters
     */
      select distinct account_id, meter_point_id, meter_id
      from public.ref_meters_attributes rma
      where (lower(rma.metersattributes_attributevalue) LIKE ('s2%')
               and lower(metersattributes_attributename) = 'metertype')
         or (lower(rma.metersattributes_attributedescription) = 'metertype'
               and lower(metersattributes_attributename) LIKE ('s2%'))
  )
    , cte_nextbilldate as (
    /*
      Filter for billdate - 5
      change the figure 5 to calculate nextbilldate - ?
     */
      select distinct substring(t.next_bill_date, 1, 10)       as nextbilldate,
                      substring(t.next_bill_date, 9, 2) :: int as Intnextbillday
      from public.ref_calculated_daily_customer_file t
      where DATEDIFF(days, substring(getdate(), 1, 10) :: timestamp,
                     substring(t.next_bill_date, 1, 10) :: timestamp) = 5
  )
    , cte_Internal_Readings_Final_Elec as (
      select stg.*, row_number() over (partition by account_id, meterpointnumber order by COALESCE(supplyenddate,
                                                                                                   getdate()) desc) as RowID
      from (select distinct riv.*
            from public.ref_meterpoints riv
                   inner join cte_smart_meters sm on sm.account_id = riv.account_id
                                                       and sm.meter_point_id = riv.meter_point_id
                                                       and riv.meterpointtype = 'E') stg
  )
    , cte_elec_DCF as (
      select dcf.*
      from public.ref_calculated_daily_customer_file dcf
          -- where substring(dcf.next_bill_date, 1, 10)  in (select nextbilldate from cte_nextbilldate)
      where substring(dcf.next_bill_date, 9, 2) :: int in (select Intnextbillday from cte_nextbilldate)
        and lower(dcf.supply_type) in ('dual', 'elec')
        and lower(dcf.elec_reg_status) in ('live')
  )
    , cte_smart_reads as (
      select irfe.*, dcf.next_bill_date
      from cte_Internal_Readings_Final_Elec irfe
             inner join cte_elec_DCF dcf on irfe.account_id = dcf.account_id
                                              and irfe.RowID = 1
  )
    , cte_metering_portfolio_elec as (
      select *, row_number() over (partition by account_id, meterpointnumber order by COALESCE(supplyenddate,
                                                                                               getdate()) desc) as RecID
      from cte_smart_reads
  )
    , cte_qry1 as (select rrsd.account_id,
                          rrsd.meterpoint_id,
                          rrsd.meter_id,
                          rrsd.register_id,
                          cmpe.meterpointnumber as mpxn,
                          rrsd.deviceid,
                          rrsd.total_consumption,
                          rrsd.type,
                          rrsd.register_num,
                          rrsd.register_value,
                          rrsd.timestamp,
                          cmpe.meterpointtype      supply_type,
                          cmpe.next_bill_date
                   from public.ref_readings_smart_daily rrsd
                          right join cte_metering_portfolio_elec cmpe on rrsd.account_id = cmpe.account_id
                                                                           and rrsd.mpxn = cmpe.meterpointnumber
                                                                           and cmpe.RecID = 1
                          where rrsd.account_id not in  (select distinct account_id
                                                          from (
                                                              select account_id, count(distinct register_id) as cnt
                                                              from public.ref_readings_smart_daily
                                                              group by account_id ) stg
                                                          where stg.cnt > 1
                                                        )
                                                                             order by 1, 2, 3)
    , cte_qry2 as (
      select *, row_number() over (partition by account_id, mpxn order by timestamp desc) as RowID
      from cte_qry1
  )
  select distinct deviceid          as ManualMeterReadingId,
                  account_id        as accountID,
                  timestamp         as meterReadingDateTime,
                  type              as meterType,
                  mpxn              as meterPointNumber,
                  meter_id          as meter,
                  register_id       as register,
                  total_consumption as reading,
                  register_value,
                  'SMART'           as source,
                  NULL              as createdBy,
                  RowID
                  next_bill_date
  from cte_qry2
where RowID=1
