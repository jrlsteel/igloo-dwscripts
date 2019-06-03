drop view vw_ref_readings_all;

create view vw_ref_readings_all
  as
    SELECT account_id,
           meter_point_id,
           meter_id,
           meter_reading_id,
           register_id,
           register_reading_id,
           billable,
           haslivecharge,
           hasregisteradvance,
           meterpointnumber,
           meterpointtype,
           meterreadingcreateddate,
           meterreadingdatetime,
           meterreadingsourceuid,
           meterreadingstatusuid,
           meterreadingtypeuid,
           meterserialnumber,
           readingvalue,
           registerreference,
           required
    FROM ref_readings_internal_nosi
    union
    SELECT account_id,
           meter_point_id,
           meter_id,
           meter_reading_id,
           register_id,
           register_reading_id,
           billable,
           haslivecharge,
           hasregisteradvance,
           meterpointnumber,
           meterpointtype,
           meterreadingcreateddate,
           meterreadingdatetime,
           meterreadingsourceuid,
           meterreadingstatusuid,
           meterreadingtypeuid,
           meterserialnumber,
           readingvalue,
           registerreference,
           required
    FROM ref_readings_internal;




