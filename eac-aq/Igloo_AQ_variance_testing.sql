select
       account_id,
       register_id,
       mprn,

       --dates
       date_open_read as d1_igloo,
       nrl_open_date as d1_ensek,
       date_close_read as d2,
       most_recent_read as d3_igloo,
       (select top 1 aq_calc_period_end from ref_nrl
        where supply_meter_point_reference = aq_components.mprn and aq_calc_period_end notnull
        order by aq_calc_period_end desc) as d3_ensek,

       --values
       value_open_read as v1_igloo,
       start_reading as v1_ensek,
       value_close_read as v2,
       most_recent_reading_value as v3_igloo,
       null as v3_ensek,

       --advances
       meter_advance as a12_igloo,
       value_close_read-start_reading as a12_ensek,
       meter_advance2 as a23_igloo,
       null as a23_ensek,

       --day_diffs
       days_diff as days12_igloo,
       datediff(days,nrl_open_date,date_close_read) as days12_ensek,
       datediff(days,date_close_read,most_recent_read) as days23_igloo,
       null as days23_ensek,

       --aq estimations
       Xoserve_prev_AQ as aq_prev_ensek,
       meter_advance * 1.02264 * avg_cv * u * (1/3.6) * 365 / CWAALP as aq2_igloo,
       Xoserve_AQ as aq2_ensek,
       meter_advance2 * 1.02264 * avg_cv2 * u * (1/3.6) * 365 / CWAALP2 as aq3_igloo,
--         (select top 1 estimation_value from ref_estimates_gas_internal regi
--         where regi.effective_from < aq_components.AQ_app_from and regi.mprn = aq_components.mprn
--         order by regi.effective_from desc) as prev_aq
    (select top 1 revised_supply_meter_point_aq from ref_nrl
    where supply_meter_point_reference = aq_components.mprn and aq_calc_period_end notnull
    order by aq_calc_period_end desc) as aq3_ensek,

       --actual consumption
       year_on_date as cons_end,
       datediff(days,date_close_read,year_on_date) as days_year_diff,
       year_on_val - value_close_read as meter_advance,
       (year_on_val - value_close_read) * 1.02264 * u * (1/3.6) *
            (select 0.5 * avg(cv.value)
            from ref_alp_igloo_cv cv
            where cv.ldz = aq_components.ldz
                and cv.applicable_for between aq_components.date_close_read
                    and aq_components.year_on_date-1
            ) as consumption,

       --other
       meterreadingsourceuid as source

from (
    select distinct
        reads_oc.account_id,
        reads_oc.register_id,
        reads_oc.ldz,
        reads_oc.u,
        reads_oc.closing_reading_date as date_close_read,
        reads_oc.closing_reading_value as value_close_read,
        (select max(r.meter_reading_id) from temp_readings_all r
         where r.register_id = reads_oc.register_id and r.meterreadingdatetime = reads_oc.closing_reading_date) as id_close_read,
        reads_oc.read_date as date_open_read,
        reads_oc.read_value as value_open_read,
        reads_oc.meter_reading_id as id_open_reading,
        reads_oc.AQ_app_from,
        reads_oc.days_diff,
        reads_oc.ind_AQ as Xoserve_AQ,
        reads_oc.prev_ind_AQ as Xoserve_prev_AQ,
        reads_oc.aq_calc_period_start as nrl_open_date,
        reads_oc.start_reading,
        reads_oc.year_on_date,
        (select top 1 readingvalue from temp_readings_all rrav where rrav.meterpointnumber = reads_oc.mprn
                    and rrav.account_id = reads_oc.account_id
                    and rrav.register_id = reads_oc.register_id
                    and rrav.meterreadingdatetime = year_on_date) as year_on_val,


        (case when reads_oc.read_value > reads_oc.closing_reading_value --rollover
            then 1 + pow(10,len(reads_oc.read_value))
            else 0 end) +
            reads_oc.closing_reading_value - reads_oc.read_value                                        as meter_advance,
        (select 0.5 * avg(cv.value) from ref_alp_igloo_cv cv where cv.ldz = reads_oc.ldz
                and cv.applicable_for between reads_oc.read_date and reads_oc.closing_reading_date-1)   as avg_cv,
        (select sum((1 + (waalp.value * waalp.variance * 0.5)) * waalp.forecastdocumentation)
            from ref_alp_igloo_daf_wcf waalp where waalp.ldz = reads_oc.ldz
                and waalp.date between reads_oc.read_date and reads_oc.closing_reading_date-1)          as CWAALP,

        (case when reads_oc.closing_reading_value > reads_oc.most_recent_reading_value --rollover
            then 1 + pow(10,len(reads_oc.closing_reading_value))
            else 0 end) +
            reads_oc.most_recent_reading_value - reads_oc.closing_reading_value                         as meter_advance2,
        (select 0.5 * avg(cv.value) from ref_alp_igloo_cv cv where cv.ldz = reads_oc.ldz
                and cv.applicable_for between reads_oc.closing_reading_date and reads_oc.most_recent_read-1)   as avg_cv2,
        (select sum((1 + (waalp.value * waalp.variance * 0.5)) * waalp.forecastdocumentation)
            from ref_alp_igloo_daf_wcf waalp where waalp.ldz = reads_oc.ldz
                and waalp.date between reads_oc.closing_reading_date and reads_oc.most_recent_read-1)   as CWAALP2,
        reads_oc.most_recent_read,
        reads_oc.most_recent_reading_value,

        reads_oc.mprn,
        reads_oc.meterreadingsourceuid

    from
        (select
            reads.*,
            nrl.*,
            datediff(days,reads.read_date,reads.closing_reading_date) as days_diff,

            -- METHOD 2 - 12m through to 9m, then 12m through to 3y
            open_read_suitability_score(datediff(days,reads.read_date,reads.closing_reading_date),2)   as selection_rank_2,
            min(open_read_suitability_score(datediff(days,reads.read_date,reads.closing_reading_date),2))
                over (partition by reads.AQ_app_from,reads.register_id)                     as selection_rank_2_min,

            (select max(r.readingvalue)
             from temp_readings_all r
             where r.register_id = reads.register_id
               and r.meterreadingdatetime = reads.closing_reading_date) as closing_reading_value,

            (select max(r.readingvalue)
             from temp_readings_all r
             where r.register_id = reads.register_id
               and r.meterreadingdatetime = reads.most_recent_read) as most_recent_reading_value,

            (select
                    min(rrav.meterreadingdatetime)
                from temp_readings_all rrav
                where rrav.meterpointnumber = reads.mprn
                    and rrav.account_id = reads.account_id
                    and rrav.register_id = reads.register_id
                    and rrav.meterreadingdatetime >= reads.closing_reading_date + 365
                ) as year_on_date
        from
            (select
                regi.*,

                rriv.account_id,
                rriv.register_id,
                rriv.read_value,
                rriv.read_date,
                rriv.meter_point_id,
                rriv.meter_reading_id,
                rriv.meterreadingsourceuid,

                trim(rma_ldz.attributes_attributevalue) as ldz,
                case when rma_imp.attributes_attributevalue = 'Y' then 2.83
                else case when rma_imp.attributes_attributevalue = 'N' then 1
                end end as u,

                (select
                    max(rrav.meterreadingdatetime)
                from temp_readings_all rrav
                where rrav.meterpointnumber = regi.mprn
                    and rrav.account_id = rriv.account_id
                    and rrav.register_id = rriv.register_id
                ) as most_recent_read,

                (select
                    max(rriv_close.meterreadingdatetime)
                from temp_readings_all rriv_close
                where rriv_close.meterreadingdatetime <= regi.AQ_app_from
                    and rriv_close.meterpointnumber = regi.mprn
                    and rriv_close.meterpointtype = 'G'
                    and rriv_close.register_id = rriv.register_id
                    and rriv_close.meterreadingdatetime between regi.read_window_start and regi.read_window_end
                ) as closing_reading_date

            from
                 -- Get AQ estimations from ref_estimates_gas_internal
                (select
                    lead(estimation_value) over (partition by register_id, account_id order by effective_from) as ind_AQ,
                    estimation_value as prev_ind_AQ,
                    effective_from as AQ_app_from,

                    last_day(add_months(effective_from,-1)) + 10 as read_window_end,
                    last_day(add_months(effective_from,-2)) + 11 as read_window_start,
                    mprn
                from
                    ref_estimates_gas_internal regi
                where date_part(d,AQ_app_from)>17
                ) regi
                -- join readings for the relevant meter point
                inner join
                (select
                    account_id,
                    register_id,
                    readingvalue as read_value,
                    meterreadingdatetime as read_date,
                    meterpointnumber as mprn,
                    meter_point_id,
                    meterpointtype,
                    meter_reading_id,
                    meterreadingsourceuid
                from temp_readings_all
                ) rriv
                on rriv.mprn = regi.mprn
                -- join register details
                inner join ref_meterpoints_attributes rma_ldz
                    on rriv.meter_point_id = rma_ldz.meter_point_id
                        and rma_ldz.attributes_attributename = 'LDZ'
                inner join ref_meterpoints_attributes rma_imp
                    on rriv.meter_point_id = rma_imp.meter_point_id
                        and rma_imp.attributes_attributename = 'Gas_Imperial_Meter_Indicator'

            where datediff(days,read_date,closing_reading_date)>=273
            ) reads
            inner join ref_nrl nrl
                    on nrl.supply_meter_point_reference = reads.mprn
                        --and last_day(nrl.file_date) = last_day(regi.AQ_app_from)
                        and nrl.aq_calc_period_end = reads.closing_reading_date
        where datediff(days, closing_reading_date, most_recent_read) >= 273
        ) reads_oc
    where reads_oc.selection_rank_2 = reads_oc.selection_rank_2_min and reads_oc.read_date > '2014-10-01'
) aq_components
where aq_components.Xoserve_AQ > 20