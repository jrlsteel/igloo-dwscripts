create or replace view vw_etl_smart_billing_reads_gas as
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
       , cte_meterpoints_register as (
        select mp.account_id, mp.meterpointnumber, mp.meter_point_id, reg.meter_id, reg.register_id, mp.meterpointtype
        from public.ref_meterpoints mp
                 inner join public.ref_registers reg on mp.meter_point_id = reg.meter_point_id
            and mp.account_id = reg.account_id
    )
       , cte_smet_innventory as (
        SELECT distinct rf.account_id     as inventory_account_id,
                        rf.meter_point_id as inventory_meter_point_id,
                        rf.register_id    as inventory_register_id,
                        rf.meter_id       as inventory_meter_id,
                        t.dspinventory_gsme_importmpxn,
                        t.mpxn            as inventory_mpxn,
                        t.dspinventory_gsme_uprn,
                        t.dspinventory_gsme_deviceid,
                        t.dspinventory_gpf_deviceid
        FROM public.ref_smart_inventory t
                 left join cte_meterpoints_register rf on t.dspinventory_gsme_importmpxn = rf.meterpointnumber
            and rf.meterpointtype = 'G'
    )
       , cte_smet_gas_reads as (
        select read.*,
               csi.dspinventory_gsme_importmpxn as gsme_mpxn,
               csi.inventory_account_id,
               csi.inventory_meter_point_id,
               csi.inventory_register_id,
               csi.inventory_meter_id
        from public.ref_readings_smart_daily read
                 inner join cte_smet_innventory csi on csi.dspinventory_gpf_deviceid = read.deviceid
        where lower(read.type) = 'gas'
    )
       , cte_Internal_Readings_Final_Gas as (
        select stg.*,
               row_number() over (partition by account_id, meterpointnumber order by COALESCE(supplyenddate,
                                                                                              getdate()) desc) as RowID
        from (select distinct riv.*
              from public.ref_meterpoints riv
                       inner join cte_smart_meters sm on sm.account_id = riv.account_id
                  --and sm.meter_point_id = riv.meter_point_id
                  and riv.meterpointtype = 'G') stg
    )
       , cte_gas_DCF as (
        select dcf.*
        from public.ref_calculated_daily_customer_file dcf
             -- where substring(dcf.next_bill_date, 1, 10)  in (select nextbilldate from cte_nextbilldate)
        where substring(dcf.next_bill_date, 9, 2) :: int in (select Intnextbillday from cte_nextbilldate)
          and lower(dcf.supply_type) in ('dual', 'gas')
          and lower(dcf.elec_reg_status) in ('live', 'final')
    )
       , cte_smart_reads as (
        select irfe.*, dcf.next_bill_date
        from cte_Internal_Readings_Final_Gas irfe
                 inner join cte_gas_DCF dcf on irfe.account_id = dcf.account_id
            and irfe.RowID = 1
    )
       , cte_metering_portfolio_gas as (
        select *,
               row_number() over (partition by account_id, meterpointnumber order by COALESCE(supplyenddate,
                                                                                              getdate()) desc) as RecID
        from cte_smart_reads
    )
       , cte_qry1 as (select rrsd.inventory_account_id     as account_id,
                             rrsd.inventory_meter_point_id as meterpoint_id,
                             rrsd.inventory_meter_id       as meter_id,
                             rrsd.inventory_register_id    as register_id,
                             cmpe.meterpointnumber         as mpxn,
                             rrsd.deviceid,
                             rrsd.total_consumption,
                             rrsd.type,
                             rrsd.register_num,
                             rrsd.register_value,
                             rrsd.timestamp,
                             cmpe.meterpointtype              supply_type,
                             cmpe.next_bill_date
                      from cte_smet_gas_reads rrsd
                               right join cte_metering_portfolio_gas cmpe on rrsd.inventory_account_id = cmpe.account_id
                          and rrsd.gsme_mpxn = cmpe.meterpointnumber
                          and cmpe.RecID = 1
                      order by 1, 2, 3)
       , cte_qry2 as (
        select *, row_number() over (partition by account_id, mpxn order by timestamp desc) as RowID
        from cte_qry1
    )
       ,
        cte_mpxn_mpid_mapping as (
            select account_id, meterpointnumber, max(meter_point_id) as meter_point_id
            from public.ref_meterpoints
            group by account_id, meterpointnumber
        )
    select distinct cq2.deviceid                as ManualMeterReadingId,
                    cq2.account_id              as accountID,
                    cq2.timestamp               as meterReadingDateTime,
                    cq2.type                    as meterType,
--                     cq2.mpxn              as meterPointNumber,
                    temp_mapping.meter_point_id as meterPointNumber,
                    cq2.meter_id                as meter,
                    cq2.register_id             as register,
                    cq2.total_consumption       as reading,
                    'SMART'                     as source,
                    NULL                        as createdBy,
                    cq2.next_bill_date
    from cte_qry2 cq2
             left join cte_mpxn_mpid_mapping temp_mapping on cq2.account_id = temp_mapping.account_id and
                                                             cq2.mpxn = temp_mapping.meterpointnumber
    where RowID = 1
    with no schema binding;

alter table vw_etl_smart_billing_reads_gas
    owner to igloo;

