insert into ref_meterpoints_audit (account_id,
                                   meter_point_id,
                                   meterpointnumber,
                                   associationstartdate,
                                   associationenddate,
                                   supplystartdate,
                                   supplyenddate,
                                   issmart,
                                   issmartcommunicating,
                                   meterpointtype,
                                   etlchangetype,
                                   etlchange)
select
       rm.account_id,
       rm.meter_point_id,
       rm.meterpointnumber,
       rm.associationstartdate,
       rm.associationenddate,
       rm.supplystartdate,
       rm.supplyenddate,
       rm.issmart,
       rm.issmartcommunicating,
       rm.meterpointtype,
       'u' as etlchangetype,
       current_timestamp as etlchange
from
    (select *, row_number() over (partition by account_id, meter_point_id order by etlchange desc) as rn
    from ref_meterpoints_audit) latest_audit
    inner join ref_meterpoints rm
        on latest_audit.account_id = rm.account_id and latest_audit.meter_point_id = rm.meter_point_id
where latest_audit.rn = 1 and not
      (
       latest_audit.account_id = rm.account_id and
       latest_audit.meter_point_id = rm.meter_point_id and
       latest_audit.meterpointnumber = rm.meterpointnumber and
       nvl(latest_audit.associationstartdate, '1970-01-01') = nvl(rm.associationstartdate, '1970-01-01') and
       nvl(latest_audit.associationenddate, '1970-01-01') = nvl(rm.associationenddate, '1970-01-01') and
       nvl(latest_audit.supplystartdate, '1970-01-01') = nvl(rm.supplystartdate, '1970-01-01') and
       nvl(latest_audit.supplyenddate, '1970-01-01') = nvl(rm.supplyenddate, '1970-01-01') and
       latest_audit.issmart = rm.issmart and
       latest_audit.issmartcommunicating = rm.issmartcommunicating and
       latest_audit.meterpointtype = rm.meterpointtype);