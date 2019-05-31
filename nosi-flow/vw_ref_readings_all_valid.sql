drop view vw_ref_readings_all_valid;

create view vw_ref_readings_all_valid as
SELECT s.account_id,
       s.meter_point_id,
       s.meter_id,
       s.meter_reading_id,
       s.register_id,
       s.register_reading_id,
       s.billable,
       s.haslivecharge,
       s.hasregisteradvance,
       s.meterpointnumber,
       s.meterpointtype,
       s.meterreadingcreateddate,
       s.meterreadingdatetime,
       s.meterreadingsourceuid,
       s.meterreadingstatusuid,
       s.meterreadingtypeuid,
       s.meterserialnumber,
       s.registerreference,
       s.required,
       s.no_of_digits,
       s.readingvalue,
       s.previous_reading,
       s.current_reading,
       s.max_previous_reading,
       s.max_reading,
       round_the_clock_reading_check_digits_v1(s.current_reading, s.previous_reading, s.max_reading,
                                               CASE
                                                 WHEN (s.max_previous_reading <> s.current_reading
                                                         and max_previous_reading > max_reading - 10000
                                                   ) THEN 'Y'
                                                 ELSE 'N' END) corrected_reading,
       CASE
         WHEN (s.max_previous_reading <> s.current_reading
                 and max_previous_reading > max_reading - 10000
           ) THEN 'Y' :: text
         ELSE 'N' :: text END AS                               meter_rolled_over,
       getdate()              as                               etlchange
FROM (SELECT ri.account_id,
             ri.meter_point_id,
             ri.meter_id,
             ri.meter_reading_id,
             ri.register_id,
             ri.register_reading_id,
             ri.billable,
             ri.haslivecharge,
             ri.hasregisteradvance,
             ri.meterpointnumber,
             ri.meterpointtype,
             ri.meterreadingcreateddate,
             ri.meterreadingdatetime,
             ri.meterreadingsourceuid,
             ri.meterreadingstatusuid,
             ri.meterreadingtypeuid,
             ri.meterserialnumber,
             ri.readingvalue,
             ri.registerreference,
             ri.required,
             COALESCE((rega.registersattributes_attributevalue) :: integer, 0) AS no_of_digits,
             COALESCE(pg_catalog.lead(ri.readingvalue,
                                      1) OVER (PARTITION BY ri.account_id, ri.register_id ORDER BY ri.meterreadingdatetime DESC),
                      (0) :: double precision)                                 AS previous_reading,
             COALESCE(ri.readingvalue, (0) :: double precision)                AS current_reading,
             (power((10) :: double precision,
                    (COALESCE((rega.registersattributes_attributevalue) :: integer, 0)) :: double precision) -
              (1) :: double precision)                                         AS max_reading,
             max(ri.readingvalue) OVER (PARTITION BY ri.account_id, ri.register_id ORDER BY ri.meterreadingdatetime DESC
               ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)               AS max_previous_reading
      FROM (vw_ref_readings_all ri
          LEFT JOIN ref_registers_attributes rega ON ((
        (rega.register_id = ri.register_id) AND rega.account_id = ri.account_id and
        ((rega.registersattributes_attributename) :: text = ('No_Of_Digits' :: character varying) :: text))))
      WHERE ((((((ri.meterreadingsourceuid) :: text = 'CUSTOMER' :: text) AND
                ((ri.meterreadingstatusuid) :: text = 'VALID' :: text)) AND
               ((ri.meterreadingtypeuid) :: text = 'ACTUAL' :: text)) AND (ri.billable = true)) OR
             (((((ri.meterreadingsourceuid) :: text = 'DC' :: text) AND
                ((ri.meterreadingstatusuid) :: text = 'VALID' :: text)) AND
               ((ri.meterreadingtypeuid) :: text = 'ACTUAL' :: text)) AND (ri.billable = true))
               OR
             (((((ri.meterreadingsourceuid) :: text = 'DCOPENING' :: text) AND
                ((ri.meterreadingstatusuid) :: text = 'VALID' :: text)) AND
               ((ri.meterreadingtypeuid) :: text = 'ACTUAL' :: text)) AND (ri.billable = true)))
         or ri.meterreadingsourceuid :: text = 'NOSI'
      ORDER BY ri.account_id, ri.register_id, ri.meterreadingdatetime) s;

select * from vw_ref_readings_all_valid
where meterreadingsourceuid = 'NOSI';
