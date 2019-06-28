select
    primary_stats.registrations,
    primary_stats.cancelled,
    primary_stats.loss,
    primary_stats.registrations - primary_stats.cancelled - primary_stats.loss      as net_closing_acounts,

    round(100*(primary_stats.cancelled * 1.0 / primary_stats.registrations),2)            as cancellation_rate,
    round(100*(primary_stats.loss * 1.0 / primary_stats.registrations),2)                 as loss_rate,

    primary_stats.pending_live,
    primary_stats.live,
    primary_stats.pending_final,
    primary_stats.live + primary_stats.pending_final                                as live_and_pending_final,
    primary_stats.pending_live + primary_stats.live + primary_stats.pending_final   as live_and_pending,

    primary_stats.home_moves                                                        as home_move_gains,
    primary_stats.cot                                                               as cot_losses

from (
    select
        (select count(distinct account_id)
         from ref_meterpoints
         where associationstartdate < supplystartdate
        )                                                                               as registrations,
        (select count(distinct account_id)
         from ref_meterpoints
         where associationstartdate < supplystartdate
        ) - (count(distinct account_id) - sum(home_move_in))                            as cancelled,
        sum(case when loss_type='COS' and acc_stat='Final' then 1 else 0 end)           as loss,
        sum(case when acc_stat='Pending Live' then 1 else 0 end)                        as pending_live,
        sum(case when acc_stat='Live' then 1 else 0 end)                                as live,
        sum(case when acc_stat='Pending Final' then 1 else 0 end)                       as pending_final,

        sum(case when loss_type='COT' and acc_stat='Final' then 1 else 0 end)           as cot,
        sum(home_move_in)                                                               as home_moves


    from (
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
    ) full_as
) primary_stats