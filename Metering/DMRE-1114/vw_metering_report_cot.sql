
create view vw_metering_report_cot as

select account_id,
       meter_point_id,
       meterpointtype,
       greatest(supplystartdate, associationstartdate)                                      as gain_date,
       case when supplystartdate < associationstartdate then 'CoT_Gain' else 'CoS_Gain' end as gain_type,
       least(supplyenddate, associationenddate)                                             as loss_date,
       case
           when loss_date is not null then
               case
                   when associationenddate is null then 'CoS_Loss'
                   when supplyenddate is null then 'CoT_Loss'
                   -- if we get to here, we know both end dates are not null
                   when associationenddate < supplyenddate then 'CoT_Loss'
                   else 'CoS_Loss'
                   end
           end                                                                              as loss_type
from ref_meterpoints
order by meter_point_id, account_id
