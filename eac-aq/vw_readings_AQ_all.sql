drop view vw_readings_AQ_all;
create or replace view vw_readings_AQ_all as

/*drop table temp_readings_all;
create table temp_readings_all as*/
select account_id,
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
                row_number() over (partition by account_id, register_id, meterreadingdatetime
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

                  select distinct nrl.account_id,
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
                  from ref_readings_internal_nrl nrl
                           inner join (select account_id, nrl_date, nrl_value, max(apd) as max_apd
                                       from (
                                                select nrl.*,
                                                       nrl.readingvalue                                                    as nrl_value,
                                                       nrl.meterreadingdatetime                                            as nrl_date,
                                                       rriv.readingvalue                                                   as rriv_value,
                                                       rriv_value - nrl_value + case
                                                                                    when rriv_value < nrl_value
                                                                                        then pow(10, coalesce(
                                                                                            nullif(rriv.no_of_digits, 0),
                                                                                            greatest(len(nrl_value), 5)))
                                                                                    else 0 end                             as advance,
                                                       datediff(days, nrl.meterreadingdatetime, rriv.meterreadingdatetime) as days_diff,
                                                       advance / days_diff                                                 as apd
                                                from ref_readings_internal_nrl nrl
                                                         inner join ref_readings_internal_valid rriv
                                                                    on rriv.meterreadingdatetime > nrl.meterreadingdatetime
                                                                        and rriv.account_id = nrl.account_id
                                                                        and
                                                                       rriv.meterserialnumber = nrl.meterserialnumber
                                                                        and rriv.meterpointtype = 'G'
                                                where days_diff >= 14
                                            ) nrl_stats
                                       group by account_id, nrl_date, nrl_value
                                       having max_apd < 50) valid_nrl
                                      on nrl.account_id = valid_nrl.account_id and
                                         nrl.meterreadingdatetime = valid_nrl.nrl_date and
                                         nrl.readingvalue = valid_nrl.nrl_value
                  where readingvalue notnull
              ) readings_all

         where readings_all.readingvalue notnull
           and meterpointtype = 'G'
     ) ranked
where uniqueness_rank = 1
order by account_id, register_id, meterreadingdatetime