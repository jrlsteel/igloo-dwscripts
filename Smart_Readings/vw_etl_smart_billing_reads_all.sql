   create or replace view vw_etl_smart_billing_reads_all as  SELECT manualmeterreadingid,
            accountid,
            meterreadingdatetime,
            metertype,
            meterpointnumber,
            meter,
            register,
            reading,
            source,
            createdby,
            next_bill_date
     FROM vw_etl_smart_billing_reads_elec
     WHERE dateadd(days,5, meterreadingdatetime) = next_bill_date
     AND   substring(getdate(), 1, 10) :: timestamp = meterreadingdatetime
     union
     SELECT manualmeterreadingid,
            accountid,
            meterreadingdatetime,
            metertype,
            meterpointnumber,
            meter,
            register,
            reading,
            source,
            createdby,
            next_bill_date
     FROM vw_etl_smart_billing_reads_gas
     WHERE dateadd(days,5, meterreadingdatetime) = next_bill_date
     AND   substring(getdate(), 1, 10) :: timestamp = meterreadingdatetime
     ORDER BY accountid