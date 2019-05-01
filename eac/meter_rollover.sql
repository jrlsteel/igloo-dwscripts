/******* ref_readings_internal_valid ********/

--drop table ref_readings_internal_valid;
create table ref_readings_internal_valid
(
  account_id              bigint encode delta32k distkey,
  meter_point_id          bigint encode delta32k,
  meter_id                bigint encode delta32k,
  meter_reading_id        bigint,
  register_id             bigint encode delta32k,
  register_reading_id     bigint,
  billable                boolean,
  haslivecharge           boolean,
  hasregisteradvance      boolean,
  meterpointnumber        bigint,
  meterpointtype          varchar(1) encode bytedict,
  meterreadingcreateddate timestamp,
  meterreadingdatetime    timestamp encode bytedict,
  meterreadingsourceuid   varchar(255),
  meterreadingstatusuid   varchar(255),
  meterreadingtypeuid     varchar(255),
  meterserialnumber       varchar(255),
  registerreference       varchar(255),
  required                boolean,
  no_of_digits            integer,
  readingvalue            double precision,
  previous_reading        double precision,
  current_reading         double precision,
  max_previous_reading    double precision,
  max_reading             double precision,
  corrected_reading       double precision,
  meter_rolled_over       varchar(1),
  etlchange               timestamp
)
  diststyle key
  sortkey (account_id, meter_point_id, meter_id, meterreadingcreateddate, meterreadingdatetime);

alter table ref_readings_internal_valid
  owner to igloo;

-- delete from ref_readings_internal_valid;

-- insert into ref_readings_internal_valid (
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
       current_timestamp      AS                               etlchange
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
             "max"(
               ri.readingvalue) OVER (PARTITION BY ri.account_id, ri.register_id ORDER BY ri.meterreadingdatetime DESC
               ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)               AS max_previous_reading
      FROM (ref_readings_internal ri
          LEFT JOIN ref_registers_attributes rega ON ((
        (rega.register_id = ri.register_id) AND rega.account_id = ri.account_id and
        ((rega.registersattributes_attributename) :: text = ('No_Of_Digits' :: character varying) :: text))))
      WHERE ((((((ri.meterreadingsourceuid) :: text = 'CUSTOMER' :: text) AND
                ((ri.meterreadingstatusuid) :: text = 'VALID' :: text)) AND
               ((ri.meterreadingtypeuid) :: text = 'ACTUAL' :: text)) AND (ri.billable = true)) OR
             (((((ri.meterreadingsourceuid) :: text = 'DC' :: text) AND
                ((ri.meterreadingstatusuid) :: text = 'VALID' :: text)) AND
               ((ri.meterreadingtypeuid) :: text = 'ACTUAL' :: text)) AND (ri.billable = true)))
      ORDER BY ri.account_id, ri.register_id, ri.meterreadingdatetime) s
--     )
;

/***** view for readings_internal_valid_pa for PA_EAC or EAV_v1 on demand ****/
-- drop view vw_corrected_round_clock_reading_pa;
create or replace view vw_corrected_round_clock_reading_pa as
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
       current_timestamp      AS                               etlchange
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
             "max"(
               ri.readingvalue) OVER (PARTITION BY ri.account_id, ri.register_id ORDER BY ri.meterreadingdatetime DESC
               ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)               AS max_previous_reading
      FROM (ref_readings_internal_pa ri
          LEFT JOIN ref_registers_attributes rega ON ((
        (rega.register_id = ri.register_id) AND rega.account_id = ri.account_id and
        ((rega.registersattributes_attributename) :: text = ('No_Of_Digits' :: character varying) :: text))))
      WHERE ((((((ri.meterreadingsourceuid) :: text = 'CUSTOMER' :: text) AND
                ((ri.meterreadingstatusuid) :: text = 'VALID' :: text)) AND
               ((ri.meterreadingtypeuid) :: text = 'ACTUAL' :: text)) AND (ri.billable = true)) OR
             (((((ri.meterreadingsourceuid) :: text = 'DC' :: text) AND
                ((ri.meterreadingstatusuid) :: text = 'VALID' :: text)) AND
               ((ri.meterreadingtypeuid) :: text = 'ACTUAL' :: text)) AND (ri.billable = true)))
      ORDER BY ri.account_id, ri.register_id, ri.meterreadingdatetime) s
;

/******* UDF for meter roll over ********/
create or replace function round_the_clock_reading_check_digits_v1(current_reading   double precision,
                                                                   previous_reading  double precision,
                                                                   max_reading       double precision,
                                                                   meter_rolled_over character varying)
  returns double precision
  stable
  language plpythonu
as $$
	import logging
	logger = logging.getLogger()
	logger.setLevel(logging.INFO)

	corrected_reading = 0.0
	meter_max_reading = max_reading
	meter_goes_round_param = meter_max_reading - 1000
	read_diff = current_reading - previous_reading

	if meter_rolled_over == 'Y':
		corrected_reading = current_reading + meter_max_reading + 1
	else:
	  corrected_reading = current_reading
	return corrected_reading
$$;


/*** validate sql for meter roll over. Should always give 0 ****/
select *
from ref_readings_internal_valid_bak_26042019
where meter_rolled_over = 'Y'
  and no_of_digits in(5)
  and max_previous_reading < 99999 - 10000
    -- 			register_id = 2069 and
--     		and account_id = 14805 and
    			and meterpointtype = 'E'
order by register_id, meterreadingdatetime;

select * from ref_readings_internal_valid where account_id = 14805 and meterpointtype = 'E';
select * from ref_readings_internal_valid_bak_26042019 where account_id = 14805 and meterpointtype = 'E';

14805,
13964,
3413,
24441


