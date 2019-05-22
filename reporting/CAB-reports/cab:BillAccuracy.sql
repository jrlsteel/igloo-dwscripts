select x.meterpointtype,
       count(distinct x.account_id1) distinct_accounts,
       count(distinct x.meter_point_id) distinct_meterpoints
                      from (select su.external_id as account_id1,
                      mp.meter_point_id,
                       mp.meterpointtype,
                       mp.supplystartdate,
                       mp.supplyenddate,
                       case
                         when supplystartdate is not null and
                              supplyenddate is not null and
                              supplystartdate > supplyenddate then 1
                         else 0 end                        as has_enddate_before_startdate,
                      ri.meterreadingdatetime,
                      ast.billdayofmonth,
                      date_part('day', ri.meterreadingdatetime) as day_meter_reading_submitted,
                     (date_part('year', ri.meterreadingdatetime) || '-' || date_part('month', ri.meterreadingdatetime) || '-' || billdayofmonth)::timestamp as billdateofmonth,
                     dateadd('month',1,(date_part('year', ri.meterreadingdatetime) || '-' || date_part('month', ri.meterreadingdatetime) || '-' || billdayofmonth)::timestamp) as billdateofnextmonth

                from ref_cdb_supply_contracts su
                       inner join ref_meterpoints mp on su.external_id = mp.account_id
                       inner join ref_meters m on m.account_id = su.external_id and m.meter_point_id = mp.meter_point_id and m.removeddate is null
                       inner join ref_registers r on r.account_id = su.external_id and r.meter_id = m.meter_id
                       inner join ref_readings_internal_valid ri on r.account_id = su.external_id and ri.register_id = r.register_id
                left outer join aws_s3_stage2_extracts.stage2_accountsettings ast on cast(ast.account_id as bigint) = su.external_id
                where su.external_id is not null
                  and mp.supplystartdate is not null
                  and dateadd(months, 12, mp.supplystartdate) <= ${date} --started supply at least before 12 months
                  and (
                      mp.supplyenddate is null -- still on supply
                        or
                      (mp.supplyenddate is not null and mp.supplyenddate >= dateadd(months, 12, mp.supplystartdate)) -- ended supply after 12 months
                      )
                     and ri.meterreadingsourceuid = 'CUSTOMER'
              ) x
  left outer join aws_s3_stage2_extracts.stage2_accounttransactions at on at.account_id = x.account_id1
         and trunc(meterreadingdatetime) = trunc(cast(creationdetail_createddate as timestamp))
-- where
--       (x.meterreadingdatetime between billdateofmonth - 7 and billdateofmonth or
--        x.meterreadingdatetime between billdateofnextmonth - 7 and billdateofnextmonth)
--        and at.transactiontype = 'BILL' and iscancelled = 'False'
--        at.account_id is not null
group by x.meterpointtype
;
