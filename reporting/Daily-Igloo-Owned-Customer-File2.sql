/*
select
    stats.account_id,
    stats.acc_stat,
    stats.home_move_in,
    case when stats.acc_stat in ('Final', 'Pending Final') then
        case when stats.aed isnull or stats.sed <= stats.aed then 'COS' else 'COT' end
        else null
    end                                                         as loss_type
from (
    select
        mp_stat.account_id,
        udf_meterpoint_status(
             min(mp_stat.start_date),
             nullif(max(mp_stat.end_date),current_date + 1000)
        )                                                           as acc_stat,
        nullif(max(mp_stat.aed),current_date + 1000)                as aed,
        nullif(max(mp_stat.sed),current_date + 1000)                as sed,
        case when sum(hmi) = count(hmi) then 1 else 0 end           as home_move_in
    from (
        select
            account_id,
            meterpointtype,
            greatest(supplystartdate, associationstartdate)             as start_date,
            coalesce(least(supplyenddate, associationenddate),
              current_date + 1000)                                      as end_date,
            coalesce(associationenddate, current_date + 1000)           as aed,
            coalesce(supplyenddate, current_date + 1000)                as sed,
            case when associationstartdate >= supplystartdate
              then 1 else 0 end                                         as hmi
        from ref_meterpoints
        where (start_date < end_date or end_date isnull) --non-cancelled meterpoints only
            --exclude known erroneous accounts
            and account_id not in (29678,36991,38044,38114,38601,38602,38603,38604,38605,38606,38607,38741,38742,41025,46605,46606)
    ) mp_stat
    group by mp_stat.account_id
    order by mp_stat.account_id
) stats


select
*,
    account_id,

from (
    select mp_stat.account_id,
        decode(mp_stat.e_or_g,'G',min(start_date),null)                     as gas_sd,
        decode(mp_stat.e_or_g,'E',min(start_date),null)                     as elec_sd,
        decode(mp_stat.e_or_g,'G',min(start_date),null)                     as gas_sd,
        decode(mp_stat.e_or_g,'E',min(start_date),null)                     as elec_sd,
        decode(mp_stat.e_or_g,'G',min(start_date),null)                     as gas_sd,
        decode(mp_stat.e_or_g,'E',min(start_date),null)                     as elec_sd,

        mp_stat.e_or_g,
        fuel_end_date,
        count(distinct case when datediff(days, end_date, fuel_end_date) < 31 --number of MPNs that were active within a month of the final fuel end date
            then meterpointnumber else 0 end)                                         as num_MPNs,
        udf_meterpoint_status(
                min(mp_stat.start_date),
                nullif(max(mp_stat.end_date), current_date + 1000)
            )                                                                         as fuel_stat
    from (
          select rm.account_id,
                 meterpointtype as e_or_g,
                 meterpointnumber,
                 greatest(supplystartdate, associationstartdate) as start_date,
                 coalesce(least(supplyenddate, associationenddate),
                          current_date + 1000)                   as end_date,
                 max(coalesce(least(supplyenddate, associationenddate),
                              current_date + 1000))
                 over (partition by rm.account_id, meterpointtype)  as fuel_end_date,
                 igloo_aq.igloo_aq_v1 as igloo_AQ,
                 industry_aq.estimation_value as industry_AQ,
                 igloo_eac.igloo_eac_v1 as igloo_EAC,
                 industry_eac.estimation_value as industry_EAC
          from ref_meterpoints rm
            left join
              (select top 1 igloo_aq_v1, account_id, meterpoint_id
              from ref_calculated_aq_v1
              group by account_id, register_id
              order by etlchange desc) igloo_aq
                  on igloo_aq.account_id = rm.account_id and igloo_aq.meterpoint_id = rm.meter_point_id
            left join
              (select top 1 estimation_value, account_id, mprn
              from ref_estimates_gas_internal
              group by account_id, register_id
              order by effective_from desc) industry_aq
                  on industry_aq.account_id = rm.account_id and industry_aq.mprn = rm.meterpointnumber
            left join
              (select top 1 igloo_eac_v1, account_id, meterpoint_id
              from ref_calculated_eac_v1
              group by account_id, register_id
              order by etlchange desc) igloo_eac
                  on igloo_eac.account_id = rm.account_id and igloo_eac.meterpoint_id = rm.meter_point_id
            left join
              (select top 1 estimation_value, account_id, mpan
              from ref_estimates_elec_internal
              group by account_id, register_id
              order by effective_from desc) industry_eac
                  on industry_eac.account_id = rm.account_id and industry_eac.mpan = rm.meterpointnumber
          where (start_date < end_date or end_date isnull) --non-cancelled meterpoints only
            --exclude known erroneous accounts
            and rm.account_id not in
                (29678, 36991, 38044, 38114, 38601, 38602, 38603, 38604, 38605, 38606, 38607, 38741, 38742,
                 41025, 46605, 46606)
        ) mp_stat
    group by mp_stat.account_id, mp_stat.e_or_g
     ) fuel_stat
group by account_id;*/


select
       coalesce(elec_stats.account_id,gas_stats.account_id) as account_id,
       elec_stats.start_date as Elec_SSD,
       gas_stats.start_date as Gas_SSD,
       elec_stats.num_meterpoints as num_elec_MPNs,
       gas_stats.num_meterpoints as num_gas_MPNs,
       elec_stats.industry_EAC as EAC_industry,
       gas_stats.industry_AQ as AQ_industry,
       elec_stats.igloo_EAC as EAC_Igloo,
       gas_stats.igloo_AQ as AQ_Igloo,
       null as reg_pay_amount,
       null as reg_pay_amount_ex_wu,
       null as reg_pay_date,
       null as projected_davings,
       null as balance,
       elec_stats.annual_spend as elec_annual_spend,
       gas_stats.annual_spend as gas_annual_spend,
       elec_stats.reg_status as elec_reg_status,
       gas_stats.reg_status as gas_reg_status,
       elec_stats.losstype as elec_loss_type,
       gas_stats.losstype as gas_loss_type,
       null as GSP,
       null as LDZ

from (select mp_elec.account_id,
             count(distinct meterpointnumber)                                as num_meterpoints,
             min(greatest(supplystartdate, associationstartdate))            as start_date,
             nullif(max(coalesce(least(supplyenddate, associationenddate),
                                 current_date + 1000)), current_date + 1000) as end_date,
             sum(igloo_eac.igloo_eac_v1)                                     as igloo_EAC,
             sum(industry_eac.estimation_value)                              as industry_EAC,
             null                                                            as annual_spend,
             udf_meterpoint_status(
                     min(greatest(supplystartdate, associationstartdate)),
                     nullif(max(coalesce(least(supplyenddate, associationenddate), current_date + 1000)),
                            current_date + 1000)
                 )                                                           as reg_status,
             null                                                            as losstype
      from (select * from ref_meterpoints where meterpointtype = 'E') mp_elec
               left join
           (select igloo_eac_v1,
                   account_id,
                   meterpoint_id,
                   row_number() over (partition by account_id, register_id order by etlchange desc) as rn
            from ref_calculated_eac_v1) igloo_eac
           on igloo_eac.account_id = mp_elec.account_id and igloo_eac.meterpoint_id = mp_elec.meter_point_id and
              igloo_eac.rn = 1
               left join
           (select top 1
                       estimation_value,
                       account_id,
                       mpan,
                       row_number() over (partition by account_id, register_id order by effective_from desc) as rn--TODO register_id here is not as you expect...
            from ref_estimates_elec_internal) industry_eac
           on industry_eac.account_id = mp_elec.account_id and industry_eac.mpan = mp_elec.meterpointnumber and
              industry_eac.rn = 1
      where (greatest(supplystartdate, associationstartdate) < least(supplyenddate, associationenddate)
          or (supplyenddate isnull and associationenddate isnull)) --non-cancelled meterpoints only
      group by mp_elec.account_id) elec_stats
         full join
     (select mp_gas.account_id,
             count(distinct meterpointnumber)                                as num_meterpoints,
             min(greatest(supplystartdate, associationstartdate))            as start_date,
             nullif(max(coalesce(least(supplyenddate, associationenddate),
                                 current_date + 1000)), current_date + 1000) as end_date,
             sum(igloo_aq.igloo_aq_v1)                                     as igloo_AQ,
             sum(industry_aq.estimation_value)                              as industry_AQ,
             null                                                            as annual_spend,
             udf_meterpoint_status(
                     min(greatest(supplystartdate, associationstartdate)),
                     nullif(max(coalesce(least(supplyenddate, associationenddate), current_date + 1000)),
                            current_date + 1000)
                 )                                                           as reg_status,
             null                                                            as losstype
      from (select * from ref_meterpoints where meterpointtype = 'G') mp_gas
               left join
           (select igloo_aq_v1,
                   account_id,
                   meterpoint_id,
                   row_number() over (partition by account_id, register_id order by etlchange desc) as rn
            from ref_calculated_aq_v1) igloo_aq
           on igloo_aq.account_id = mp_gas.account_id and igloo_aq.meterpoint_id = mp_gas.meter_point_id and
              igloo_aq.rn = 1
               left join
           (select top 1
                       estimation_value,
                       account_id,
                       mprn,
                       row_number() over (partition by account_id, register_id order by effective_from desc) as rn
            from ref_estimates_gas_internal) industry_aq
           on industry_aq.account_id = mp_gas.account_id and industry_aq.mprn = mp_gas.meterpointnumber and
              industry_aq.rn = 1
      where (greatest(supplystartdate, associationstartdate) < least(supplyenddate, associationenddate)
          or (supplyenddate isnull and associationenddate isnull)) --non-cancelled meterpoints only
      group by mp_gas.account_id) gas_stats
     on gas_stats.account_id = elec_stats.account_id


/*and mp_elec.account_id not in --exclude known erroneous accounts
    (29678, 36991, 38044, 38114, 38601, 38602, 38603, 38604, 38605, 38606, 38607, 38741, 38742,
     41025, 46605, 46606)
*/































































