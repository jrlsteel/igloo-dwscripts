create table temp_reporting_dcf as
select all_ids.account_id                                                                   as account_id,
       elec_stats.start_date                                                                as Elec_SSD,
       gas_stats.start_date                                                                 as Gas_SSD,
       elec_stats.end_date                                                                  as Elec_ED,
       gas_stats.end_date                                                                   as Gas_ED,
       least(Elec_SSD, Gas_SSD)                                                             as acc_ssd,
       case
           when (Elec_SSD is not null and Elec_ED is null) or
                (Gas_SSD is not null and Gas_ED is null) then null
           else greatest(Elec_ED, Gas_ED) end                                               as acc_ed,
       coalesce(elec_stats.num_meterpoints, 0)                                              as num_elec_MPNs,
       coalesce(gas_stats.num_meterpoints, 0)                                               as num_gas_MPNs,
       cancelled_elec.ssd                                                                   as first_canc_elec_ssd,
       cancelled_elec.sed                                                                   as first_canc_elec_sed,
       cancelled_gas.ssd                                                                    as first_canc_gas_ssd,
       cancelled_gas.sed                                                                    as first_canc_gas_sed,
       coalesce(cancelled_elec.num_mps, 0)                                                  as num_canc_elec_MPNs,
       coalesce(cancelled_gas.num_mps, 0)                                                   as num_canc_gas_MPNs,
       nullif(cons_acc_elec.ind_eac, 0)                                                     as EAC_industry,
       nullif(cons_acc_gas.ind_aq, 0)                                                       as AQ_industry,
       nullif(cons_acc_elec.ca_value, 0)                                                    as EAC_Igloo_CA,
       cons_acc_elec.ca_source                                                              as EAC_Igloo_CA_source,
       nullif(cons_acc_gas.ca_value, 0)                                                     as AQ_Igloo_CA,
       cons_acc_gas.ca_source                                                               as AQ_Igloo_CA_source,
       case
           when dd_pay_layers.wu_app_current then dd_pay_layers.dd_total
           else dd_pay_layers.dd_total - dd_pay_layers.wu_amount end                        as reg_pay_amount,
       dd_pay_layers.dd_total - dd_pay_layers.wu_amount                                     as reg_pay_amount_ex_wu,
       dd_pay_layers.wu_amount                                                              as wu_amount,
       dd_pay_layers.wu_amount > 0                                                          as wu_this_year,
       most_recent_dd.payment_day                                                           as reg_pay_date,
       q.projected_savings                                                                  as projected_savings,
       most_recent_transaction.currentbalance                                               as balance,
       ((365 * lr.elec_sc * num_elec_MPNs) + (cons_acc_elec.ind_eac * lr.elec_ur)) / 100    as elec_annual_spend,
       ((365 * lr.gas_sc * num_gas_MPNs) + (cons_acc_gas.ind_aq * lr.gas_ur)) / 100         as gas_annual_spend,
       case
           when num_elec_MPNs = 0 and num_canc_elec_MPNs > 0 then 'Cancelled'
           else elec_stats.reg_status end                                                   as elec_reg_status,
       case
           when nvl(elec_reg_status, '') != 'Cancelled' then null
           when elec_et.ET then 'ET_Gain'
           when bs.status is not null and bs.status != 'Success' then bs.status
           when cancelled_elec.rma_supply_status is not null then cancelled_elec.rma_supply_status
           else 'Generic Rejection'
           end                                                                              as elec_cancellation_reason,
       case
           when num_gas_MPNs = 0 and num_canc_gas_MPNs > 0 then 'Cancelled'
           else gas_stats.reg_status end                                                    as gas_reg_status,
       case
           when nvl(gas_reg_status, '') != 'Cancelled' then null
           when gas_et.ET then 'ET_Gain'
           when bs.status is not null and bs.status != 'Success' then bs.status
           when cancelled_gas.rma_supply_status is not null then cancelled_gas.rma_supply_status
           else 'Generic Rejection'
           end                                                                              as gas_cancellation_reason,
       case
           when elec_stats.reg_status = 'Final' then elec_stats.losstype
           else null end                                                                    as elec_loss_type,
       case
           when gas_stats.reg_status = 'Final' then gas_stats.losstype
           else null end                                                                    as gas_loss_type,
       case
           when num_elec_MPNs > 0 and num_gas_MPNs > 0 then 'Dual'
           when num_elec_MPNs > 0 and num_gas_MPNs = 0 then 'Elec'
           when num_elec_MPNs = 0 and num_gas_MPNs > 0 then 'Gas'
           else case
                    when num_canc_elec_MPNs > 0 and num_canc_gas_MPNs > 0
                        then 'Dual'
                    when num_canc_elec_MPNs > 0 and num_canc_gas_MPNs = 0
                        then 'Elec'
                    when num_canc_elec_MPNs = 0 and num_canc_gas_MPNs > 0
                        then 'Gas'
                    else 'ERROR'
               end
           end                                                                              as supply_type,
       case
           when (num_elec_MPNs + num_gas_MPNs) = 0 and
                (num_canc_elec_MPNs + num_canc_gas_MPNs) > 0 then 'Cancelled'
           when (num_elec_MPNs + num_gas_MPNs + num_canc_elec_MPNs + num_canc_gas_MPNs) = 0
               then 'ERROR'
           else udf_meterpoint_status(
                   least(Gas_SSD, Elec_SSD),
                   case supply_type
                       when 'Gas' then Gas_ED
                       when 'Elec' then Elec_ED
                       else nulls_latest(Gas_ED, Elec_ED)
                       end
               ) end                                                                        as account_status,
       case
           when account_status = 'Final' then
               case
                   when greatest(Elec_ED, Gas_ED) = Elec_ED then elec_loss_type
                   else gas_loss_type end
           end                                                                              as account_loss_type,

       elec_stats.GSP                                                                       as GSP,
       gas_stats.LDZ                                                                        as LDZ,
       case when wl1.HMI then null else sc.created_at end                                   as WL0_date,
       case when wl1.HMI then null else wl1.asd end                                         as WL1_date,
       wl1.HMI                                                                              as home_move_in,
       gas_et.ET                                                                            as gas_et1,
       elec_et.ET                                                                           as elec_et1,

       -- sales channel
       q.channel                                                                            as signup_channel,
       q.secondary_channel                                                                  as signup_channel_secondary,
       bm.map_name                                                                          as broker_name,
       bs.broker_urn,
       sc.created_at                                                                        as signup_date,
       camp.description                                                                     as campaign,
       nvl(case
               when q.electricity_usage is null then
                       (q.electricity_projected - (3.65 * q.electricity_standing)) / (q.electricity_unit / 100)
               else q.electricity_usage end, 0)                                             as quoted_eac,
       nvl(case
               when q.gas_usage is null then
                   (q.gas_projected - (3.65 * q.gas_standing)) / (q.gas_unit / 100)
               else q.gas_usage end, 0)                                                     as quoted_aq,
       q.projected_cost                                                                     as quoted_total_spend,
       datediff(days, least(Elec_SSD, Gas_SSD), payment_stats.first_payment_datetime) <= 10 as first_payment_success,
       payment_stats.num_payments                                                           as num_payments,

       payment_stats.latest_payment_datetime                                                as latest_dd_received_date,
       coalesce(latest_dd_received_date, '1970-01-01') >= dateadd(months, -1, getdate())    as payment_in_last_month,
       left(addr.postcode, len(addr.postcode) - 3)                                          as outcode,
       bill_info.first_bill_date,
       bill_info.first_bill_type,
       bill_info.latest_bill_date,
       case
           when final_bills.first_final_bill_date is not null and
                final_bills.first_final_bill_date != bill_info.latest_bill_date then 'Final_Rebill'
           else
               bill_info.latest_bill_type end                                               as latest_bill_type,
       bill_info.num_bills,
       final_bills.first_final_bill_date,
       final_bills.first_final_refund_date,
       acc_sett.billdayofmonth::int                                                         as monthly_bill_date,
       acc_sett.nextbilldate::timestamp                                                     as next_bill_date,
       case
           when acc_ssd is null then null
           when date_part(day, acc_ssd + 18) <= monthly_bill_date then
               (date_trunc('month', acc_ssd + 18) + monthly_bill_date - 1) :: timestamp
           else
               (last_day(dateadd(day, 18, acc_ssd)) + monthly_bill_date) :: timestamp
           end                                                                              as first_bill_effective_date,
       billing_performance.perf1                                                            as bill_perf_1,
       billing_performance.perf2                                                            as bill_perf_2,
       billing_performance.perf3                                                            as bill_perf_3,
       occ_acc_current.account_id is not null                                               as occupier_account,
       coalesce(occ_acc_current.days_since_cot, occ_acc_hist.days_since_cot)                as days_as_occ_acc,
       getdate()                                                                            as etlchange
from (select distinct account_id
      from ref_meterpoints_raw
      order by account_id) all_ids
         left join
     -- ELEC ------------------------------------------------------------------------------------------------ ELEC
         (select mp_elec.account_id,
                 count(distinct meterpointnumber)                     as num_meterpoints,
                 min(greatest(supplystartdate, associationstartdate)) as start_date,
                 nullif(max(coalesce(least(supplyenddate, associationenddate),
                                     current_date + 1000)),
                        current_date + 1000)                          as end_date,
                 udf_meterpoint_status(
                         min(greatest(supplystartdate, associationstartdate)),
                         nullif(max(coalesce(
                                 least(supplyenddate, associationenddate),
                                 current_date + 1000)),
                                current_date + 1000)
                     )                                                as reg_status,
                 case
                     when coalesce(max(mp_elec.associationenddate), current_date + 1000)
                         >= max(mp_elec.supplyenddate) then 'COS'
                     else 'COT' end                                   as losstype,
                 min(rma_gsp.attributes_attributevalue)               as GSP
          from (select *,
                       max(least(supplyenddate, associationenddate))
                       over (partition by account_id, meterpointtype) as mp_end_date
                from ref_meterpoints
                where meterpointtype = 'E') mp_elec
                   left join
               (select sum(igl_ind_eac)
                       over (partition by account_id, meterpoint_id, trunc(etlchange))       as igl_ind_eac,
                       account_id,
                       meterpoint_id,
                       row_number()
                       over (partition by account_id, meterpoint_id order by etlchange desc) as rn
                from ref_calculated_igl_ind_eac
                where meter_removed_date isnull) igloo_eac
               on igloo_eac.account_id = mp_elec.account_id and
                  igloo_eac.meterpoint_id = mp_elec.meter_point_id and
                  igloo_eac.rn = 1
                   left join
               (select
                    -- Sum of estimation values for a given account, mpn and day taken to cover all registers under that mpn
                    sum(estimation_value)
                    over (partition by account_id, mpan, effective_from)              as estimation_value,
                    account_id,
                    mpan,
                    row_number()
                    over (partition by account_id, mpan order by effective_from desc) as rn
                from ref_estimates_elec_internal
                where islive = true) industry_eac
               on industry_eac.account_id = mp_elec.account_id and
                  industry_eac.mpan = mp_elec.meterpointnumber and
                  industry_eac.rn = 1
                   left join ref_meterpoints_attributes rma_gsp on rma_gsp.meter_point_id = mp_elec.meter_point_id and
                                                                   rma_gsp.account_id = mp_elec.account_id and
                                                                   rma_gsp.attributes_attributename = 'GSP'
          where coalesce(
                        datediff(days, least(supplyenddate, associationenddate),
                                 mp_end_date), 0) <= 31
            and (greatest(supplystartdate, associationstartdate) <=
                 least(supplyenddate, associationenddate)
              or
                 (supplyenddate isnull and associationenddate isnull)) --non-cancelled meterpoints only
          group by mp_elec.account_id) elec_stats
     on all_ids.account_id = elec_stats.account_id
         left join vw_cons_acc_elec_all cons_acc_elec
                   on elec_stats.account_id = cons_acc_elec.account_id
         left join (select rmr_elec.account_id,
                           count(meterpointnumber)                                         num_mps,
                           min(start_date)                                              as ssd,
                           min(end_date)                                                as sed,
                           listagg(distinct rma_ss_elec.attributes_attributevalue, ',') as rma_supply_status
                    from ref_meterpoints_raw rmr_elec
                             left join ref_meterpoints_attributes rma_ss_elec
                                       on rmr_elec.account_id = rma_ss_elec.account_id and
                                          rmr_elec.meter_point_id = rma_ss_elec.meter_point_id and
                                          attributes_attributename = 'Supply_Status'
                    where meterpointtype = 'E'
                      and usage_flag = 'cancelled'
                    group by rmr_elec.account_id) cancelled_elec
                   on all_ids.account_id = cancelled_elec.account_id
         left join (select account_id,
                           listagg(distinct replace(substring(status from 22), '.', ' '), ',') as status
                    from ref_registrations_meterpoints_status_elec
                    group by account_id) rs_elec
                   on rs_elec.account_id = all_ids.account_id
         left join (select rmr.account_id,
                           sum((coalesce(attributes_attributevalue, 'N') = 'Y')::int) =
                           count(*) as et
                    from ref_meterpoints_raw rmr
                             left join ref_meterpoints_attributes rma
                                       on rma.account_id = rmr.account_id and
                                          rma.meter_point_id = rmr.meter_point_id and
                                          rma.attributes_attributename = 'ET'
                    where rmr.meterpointtype = 'E'
                    group by rmr.account_id) elec_et
                   on elec_et.account_id = all_ids.account_id
    -- ELEC ------------------------------------------------------------------------------------------------ ELEC
         left join
     -- GAS ---------------------------------------------------------------------------------------------- GAS
         (select mp_gas.account_id,
                 count(distinct meterpointnumber)                     as num_meterpoints,
                 min(greatest(supplystartdate, associationstartdate)) as start_date,
                 nullif(max(coalesce(least(supplyenddate, associationenddate),
                                     current_date + 1000)),
                        current_date + 1000)                          as end_date,
                 udf_meterpoint_status(
                         min(greatest(supplystartdate, associationstartdate)),
                         nullif(max(coalesce(
                                 least(supplyenddate, associationenddate),
                                 current_date + 1000)),
                                current_date + 1000)
                     )                                                as reg_status,
                 case
                     when coalesce(max(mp_gas.associationenddate), current_date + 1000)
                         >= max(mp_gas.supplyenddate) then 'COS'
                     else 'COT' end                                   as losstype,
                 max(mp_gas.meter_point_id)                           as gas_mpid,
                 min(rma_ldz.attributes_attributevalue)               as LDZ,
                 null                                                 as attr_ss
          from (select *,
                       max(least(supplyenddate, associationenddate))
                       over (partition by account_id, meterpointtype) as mp_end_date
                from ref_meterpoints
                where meterpointtype = 'G') mp_gas
                   left join
               (select igl_ind_aq,
                       account_id,
                       meterpoint_id,
                       row_number()
                       over (partition by account_id, register_id order by etlchange desc) as rn
                from ref_calculated_igl_ind_aq
                where meter_removed_date isnull) igloo_aq
               on igloo_aq.account_id = mp_gas.account_id and
                  igloo_aq.meterpoint_id = mp_gas.meter_point_id and
                  igloo_aq.rn = 1
                   left join
               (select estimation_value,
                       account_id,
                       mprn,
                       row_number()
                       over (partition by account_id, mprn order by effective_from desc) as rn
                from ref_estimates_gas_internal
                where islive = true) industry_aq
               on industry_aq.account_id = mp_gas.account_id and
                  industry_aq.mprn = mp_gas.meterpointnumber and
                  industry_aq.rn = 1

                   left join ref_meterpoints_attributes rma_ldz
                             on rma_ldz.meter_point_id = mp_gas.meter_point_id and
                                rma_ldz.account_id = mp_gas.account_id and
                                rma_ldz.attributes_attributename = 'LDZ'
                   left join ref_meterpoints_attributes rma_ss_gas on rma_ss_gas.account_id = mp_gas.account_id and
                                                                      rma_ss_gas.meter_point_id = mp_gas.meter_point_id and
                                                                      rma_ss_gas.attributes_attributename = 'Supply_Status'
          where coalesce(
                        datediff(days, least(supplyenddate, associationenddate),
                                 mp_end_date), 0) <= 31
            and (greatest(supplystartdate, associationstartdate) <=
                 least(supplyenddate, associationenddate)
              or
                 (supplyenddate isnull and associationenddate isnull)) --non-cancelled meterpoints only
          group by mp_gas.account_id) gas_stats
     on gas_stats.account_id = all_ids.account_id
         left join vw_cons_acc_gas_all cons_acc_gas
                   on gas_stats.account_id = cons_acc_gas.account_id
         left join (select rmr_gas.account_id,
                           count(meterpointnumber)                                     as num_mps,
                           min(start_date)                                             as ssd,
                           min(end_date)                                               as sed,
                           listagg(distinct rma_ss_gas.attributes_attributevalue, ',') as rma_supply_status
                    from ref_meterpoints_raw rmr_gas
                             left join ref_meterpoints_attributes rma_ss_gas
                                       on rmr_gas.account_id = rma_ss_gas.account_id and
                                          rmr_gas.meter_point_id = rma_ss_gas.meter_point_id and
                                          attributes_attributename = 'Supply_Status'
                    where meterpointtype = 'G'
                      and usage_flag = 'cancelled'
                    group by rmr_gas.account_id) cancelled_gas
                   on all_ids.account_id = cancelled_gas.account_id
         left join (select account_id,
                           listagg(distinct replace(substring(status from 26), '.', ' '), ',') as status
                    from ref_registrations_meterpoints_status_gas
                    group by account_id) rs_gas
                   on rs_gas.account_id = all_ids.account_id
         left join (select rmr.account_id,
                           sum((coalesce(attributes_attributevalue, 'N') = 'Y')::int) =
                           count(*) as et
                    from ref_meterpoints_raw rmr
                             left join ref_meterpoints_attributes rma
                                       on rma.account_id = rmr.account_id and
                                          rma.meter_point_id = rmr.meter_point_id and
                                          rma.attributes_attributename = 'ET'
                    where rmr.meterpointtype = 'G'
                    group by rmr.account_id) gas_et
                   on gas_et.account_id = all_ids.account_id
    -- GAS ---------------------------------------------------------------------------------------------- GAS

    -- QUOTES and BILLING ------------------------------------------------------------------------ QUOTES and BILLING
         left join (select * from ref_cdb_supply_contracts where id != 54995) sc
                   on all_ids.account_id = sc.external_id
         left join ref_cdb_registrations r on sc.registration_id = r.id
         left join ref_cdb_quotes q on q.id = r.quote_id
         left join aws_s3_stage2_extracts.stage2_cdbcampaigns camp on q.campaign_id = camp.id
         left join ref_cdb_broker_maps bm on bm.campaign_id = q.campaign_id
         left join ref_cdb_broker_signups bs on bs.registration_id = sc.registration_id and bs.id != 846
         left join vw_latest_rates lr on all_ids.account_id = lr.account_id
         left join (select account_id,
                           min(associationstartdate) as asd,
                           sum(case when associationstartdate > supplystartdate then 1 else 0 end) >
                           0                         as HMI
                    from ref_meterpoints_raw
                    group by account_id) wl1
                   on all_ids.account_id = wl1.account_id
         left join ref_cdb_addresses addr on sc.supply_address_id = addr.id

    -- STAGE 2 EXTRACTS FOR DIRECT DEBIT & ACCOUNT BALANCE ---------------------------------------------------------
    -- most recent direct debit payment (for payment day of month)
         left join (select account_id :: int                       as account_id,
                           -(amount :: double precision)           as amount,
                           date_part(day, sourcedate :: timestamp) as payment_day
                    from (
                             select *,
                                    row_number() over (partition by account_id order by sourcedate desc) as dd_rn
                             from ref_account_transactions
                             where method = 'Direct Debit'
                               and transactiontype = 'PAYMENT'
                         ) dd_with_rn
                    where dd_rn = 1) most_recent_dd
                   on most_recent_dd.account_id = all_ids.account_id
    -- most recent transaction (for account balance)
         left join (select account_id :: int as account_id,
                           currentbalance :: double precision
                    from (
                             select *,
                                    row_number()
                                    over (partition by account_id order by creationdetail_createddate desc) as rn
                             from ref_account_transactions
                         ) dd_with_rn
                    where rn = 1) most_recent_transaction
                   on most_recent_transaction.account_id = all_ids.account_id
         left join (select account_id,
                           max(creationdetail_createddate) as latest_payment_datetime,
                           min(creationdetail_createddate) as first_payment_datetime,
                           count(*)                        as num_payments
                    from (select *
                          from ref_account_transactions
                          where transactiontype = 'PAYMENT'
                            and method = 'Direct Debit') payments
                    group by account_id) payment_stats on payment_stats.account_id = all_ids.account_id
    -- payment layers for upcoming dd amount (with and without winter uplift)
         left join (select acc_end_dates.account_id,
                           sum(case
                                   when pl.payment_type_id :: integer = 5
                                       then pl.amount :: double precision
                                   else 0 end)                as wu_amount,
                           sum(case
                                   when pl.payment_type_id :: integer = 5 and
                                        pl.effective_from :: timestamp <= current_date
                                       then 1
                                   else 0 end) > 0            as wu_app_current,
                           sum(pl.amount :: double precision) as dd_total
                    from (select account_id,
                                 max(
                                         coalesce(least(associationenddate, supplyenddate), current_date)) end_date
                          from ref_meterpoints
                          group by account_id) acc_end_dates
                             inner join ref_cdb_supply_contracts sc on acc_end_dates.account_id = sc.external_id
                             inner join aws_s3_stage2_extracts.stage2_cdbpaymentlayers pl
                                        on sc.id = pl.supply_contract_id and
                                           (pl.effective_to isnull or
                                            (pl.effective_to :: timestamp) >= acc_end_dates.end_date)
                    group by acc_end_dates.account_id) dd_pay_layers
                   on dd_pay_layers.account_id = all_ids.account_id
-- Bill info
         left join (with bill_types as
                             (select account_id,
                                     current_bill_date as bill_date,
                                     case
                                         when min(num_readings) = 0 then 'No_Readings'
                                         when sum(valid_read_missing) = 0 then 'Actual'
                                         when min(valid_read_missing) = 0 then 'Partial'
                                         else 'Estimated'
                                         end           as bill_type
                              from (select bill_dates.account_id,
                                           bill_dates.current_bill_date,
                                           rri.meter_point_id,
                                           rri.meter_id,
                                           rri.register_id,
                                           count(rri.register_id)                            as num_readings,
                                           min((rri.meterreadingtypeuid = 'ESTIMATED')::int) as valid_read_missing
                                    from (select account_id,
                                                 creationdetail_createddate as current_bill_date,
                                                 nvl(lag(creationdetail_createddate)
                                                     over (partition by account_id, transactiontype order by creationdetail_createddate),
                                                     '1970-01-01')          as prev_bill_date
                                          from ref_account_transactions
                                          where transactiontype = 'BILL') bill_dates
                                             left join ref_readings_internal rri
                                                       on rri.account_id = bill_dates.account_id and
                                                          rri.meterreadingcreateddate between bill_dates.prev_bill_date and bill_dates.current_bill_date
                                    group by bill_dates.account_id,
                                             bill_dates.current_bill_date,
                                             rri.meter_point_id,
                                             rri.meter_id,
                                             rri.register_id) bill_register_read_types
                              group by account_id, current_bill_date)
                    select bill_stats.account_id,
                           first_bill_date,
                           first_bill_type.bill_type  as first_bill_type,
                           latest_bill_date,
                           latest_bill_type.bill_type as latest_bill_type,
                           num_bills
                    from (select account_id,
                                 max(creationdetail_createddate) as latest_bill_date,
                                 min(creationdetail_createddate) as first_bill_date,
                                 count(*)                        as num_bills
                          from ref_account_transactions
                          where transactiontype = 'BILL'
                          group by account_id) bill_stats
                             left join bill_types first_bill_type
                                       on first_bill_type.bill_date = bill_stats.first_bill_date and
                                          first_bill_type.account_id = bill_stats.account_id
                             left join bill_types latest_bill_type
                                       on latest_bill_type.bill_date = bill_stats.latest_bill_date and
                                          latest_bill_type.account_id = bill_stats.account_id) bill_info
                   on bill_info.account_id = all_ids.account_id
         left join (select acc_eds.account_id,
                           min(bills.creationdetail_createddate)   as first_final_bill_date,
                           min(refunds.creationdetail_createddate) as first_final_refund_date
                    from (select account_id,
                                 max(nvl(least(associationenddate, supplyenddate), getdate() + 1000)) as acc_ed
                          from ref_meterpoints
                          group by account_id
                          having acc_ed < getdate()) acc_eds
                             left join ref_account_transactions bills
                                       on bills.account_id = acc_eds.account_id and bills.transactiontype = 'BILL' and
                                          bills.creationdetail_createddate >= acc_eds.acc_ed
                             left join ref_account_transactions refunds
                                       on refunds.account_id = acc_eds.account_id and refunds.transactiontype = 'R' and
                                          refunds.creationdetail_createddate >= bills.creationdetail_createddate
                    group by acc_eds.account_id) final_bills on final_bills.account_id = all_ids.account_id
         left join (select account_id::bigint, billdayofmonth::int, nextbilldate::timestamp
                    from aws_s3_stage2_extracts.stage2_accountsettings) acc_sett
                   on acc_sett.account_id = all_ids.account_id
         left join (select bill_dates.acc_id,
                           min(case
                                   when rat.creationdetail_createddate between bd1 - 5 and bd1 + 15
                                       then datediff(days, bd1, rat.creationdetail_createddate)
                                   else null end) as perf1,
                           min(case
                                   when rat.creationdetail_createddate between bd2 - 5 and bd2 + 15
                                       then datediff(days, bd2, rat.creationdetail_createddate)
                                   else null end) as perf2,
                           min(case
                                   when rat.creationdetail_createddate between bd3 - 5 and bd3 + 15
                                       then datediff(days, bd3, rat.creationdetail_createddate)
                                   else null end) as perf3

                    from (select account_id::bigint                 as acc_id,
                                 dateadd('month', -1, nextbilldate) as most_recent_bd,
                                 case
                                     when most_recent_bd + 15 >= trunc(getdate())
                                         then dateadd('month', -1, most_recent_bd)
                                     else most_recent_bd end        as bd1,
                                 dateadd('month', -1, bd1)          as bd2,
                                 dateadd('month', -1, bd2)          as bd3
                          from (select account_id::bigint as account_id, nextbilldate::timestamp as nextbilldate
                                from aws_s3_stage2_extracts.stage2_accountsettings) acc_sett) bill_dates
                             left join ref_account_transactions rat
                                       on rat.account_id = bill_dates.acc_id and rat.transactiontype = 'BILL'
                    group by bill_dates.acc_id
                    order by bill_dates.acc_id) billing_performance on billing_performance.acc_id = all_ids.account_id
-- occupier accounts
         left join ref_occupier_accounts occ_acc_current on occ_acc_current.account_id = all_ids.account_id
         left join ref_occupier_accounts_archive occ_acc_hist on occ_acc_hist.account_id = all_ids.account_id
where all_ids.account_id not in --exclude known erroneous accounts
      (29678, 36991, 38044, 38114, 38601, 38602, 38603, 38604, 38605, 38606,
       38607, 38741, 38742,
       41025, 46605, 46606)
order by account_id