drop view vw_readings_AQ_all;
create or replace view vw_readings_AQ_all as

/*drop table temp_readings_all;
create table temp_readings_all as*/
select
    account_id,
    register_id,
    meterpointnumber,
    meter_point_id,
    meter_id,
    readingvalue,
    meterreadingdatetime,
    meterreadingsourceuid,
    meter_reading_id,
    meterpointtype,
    etlchange
from (
      select *,
             -- this rank will be 1 for any unique values and where a duplicate has occurred the values shall be taken
             -- from a table in order of ensek (ref_readings_internal_valid), nosi, nrl; whichever is present
             -- it will not distinguish between duplicates coming from the same table
             rank() over (partition by account_id, register_id, meterreadingdatetime, readingvalue
                 order by from_table /*asc*/) as uniqueness_rank

      from (
               select account_id,
                      register_id,
                      meterpointnumber,
                      meter_point_id,
                      meter_id,
                      readingvalue,
                      meterreadingdatetime,
                      meterreadingsourceuid,
                      'ensek' as from_table,
                      meter_reading_id,
                      meterpointtype,
                      etlchange
               from ref_readings_internal_valid
/*
               union

               select account_id,
                      register_id,
                      meterpointnumber,
                      meter_point_id,
                      meter_id,
                      readingvalue,
                      nullif(meterreadingdatetime,'1970-01-01'),
                      meterreadingsourceuid,
                      'nosi' as from_table,
                      meter_reading_id,
                      meterpointtype,
                      etlchange
               from ref_readings_internal_nosi
*/
               union

               select distinct account_id,
                               register_id,
                               meterpointnumber,
                               meter_point_id,
                               meter_id,
                               readingvalue,
                               meterreadingdatetime,
                               meterreadingsourceuid,
                               'nrl' as from_table,
                               meter_reading_id,
                               meterpointtype,
                                etlchange
               from ref_readings_internal_nrl
               where readingvalue notnull
           ) readings_all

      where readings_all.readingvalue notnull and meterpointtype = 'G'
  ) ranked
where uniqueness_rank = 1
order by account_id, register_id, meterreadingdatetime