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
       case q.campaign_id
           when 1 then 'Broker signups from Quotezone.'
           when 2 then 'Broker signups from MoneySuperMarket.'
           when 3 then 'Promoting our Facebook Trustpilot score.'
           when 4 then 'Broker signups from FirstHelpline.'
           when 5 then 'Broker signups from Dixons Carphone Warehouse.'
           when 6 then 'Broker signups from Energylinx.'
           when 7 then 'Refer a Friend scheme'
           when 8 then 'Promotion for EV 1200 free miles scheme'
           when 9
               then 'Customers signing up through the TeenTech link will receive a £100 reward.'
           when 10 then 'Broker signups from MoneyExpert.'
           when 11 then 'Campaign for tracking signups during the Christmas lights sponsorship'
           when 12 then 'Campaign for tracking signups via radio adverts during the Christmas lights sponsorship'
           when 13 then 'Campaign for tracking signups via QR code during the Christmas lights sponsorship'
           else q.campaign_id::varchar(3)
           end                                                                              as campaign,
       cons_acc_elec.quotes_eac                                                             as quoted_eac,
       cons_acc_gas.quotes_aq                                                               as quoted_aq,
       q.projected_cost                                                                     as quoted_total_spend,
       datediff(days, least(Elec_SSD, Gas_SSD), payment_stats.first_payment_datetime) <= 10 as first_payment_success,
       payment_stats.num_payments                                                           as num_payments,

       payment_stats.latest_payment_datetime                                                as latest_dd_received_date,
       getdate() > dateadd(months, 1, payment_stats.latest_payment_datetime) and
       (acc_ed is null or
        dateadd(months, 1, payment_stats.latest_payment_datetime) < acc_ed)                 as expected_payment_missing,
       left(addr.postcode, len(addr.postcode) - 3)                                          as outcode,
       bill_info.first_bill_date,
       bill_info.first_bill_type,
       bill_info.latest_bill_date,
       bill_info.latest_bill_type,
       bill_info.num_bills,
       final_bills.first_final_bill_date,
       final_bills.first_final_refund_date

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
         left join vw_cons_acc_elec cons_acc_elec
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
         left join vw_cons_acc_gas cons_acc_gas
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
                                         when sum(valid_read_missing) = 0 then 'Actual'
                                         when min(valid_read_missing) = 0 then 'Partial'
                                         else 'Estimated'
                                         end           as bill_type
                              from (select bill_dates.account_id,
                                           bill_dates.current_bill_date,
                                           rri.meter_point_id,
                                           rri.meter_id,
                                           rri.register_id,
                                           min((rri.meterreadingtypeuid = 'ESTIMATED')::int) as valid_read_missing
                                    from (select account_id,
                                                 creationdetail_createddate as current_bill_date,
                                                 nvl(lag(creationdetail_createddate)
                                                     over (partition by account_id, transactiontype order by creationdetail_createddate),
                                                     '1970-01-01')          as prev_bill_date
                                          from ref_account_transactions
                                          where transactiontype = 'BILL') bill_dates
                                             inner join ref_readings_internal rri
                                                        on rri.account_id = bill_dates.account_id and
                                                           rri.meterreadingdatetime between bill_dates.prev_bill_date and bill_dates.current_bill_date
                                    group by bill_dates.account_id, bill_dates.current_bill_date, rri.meter_point_id,
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
                                       on latest_bill_type.bill_date = bill_stats.first_bill_date and
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
where all_ids.account_id not in --exclude known erroneous accounts
      (29678, 36991, 38044, 38114, 38601, 38602, 38603, 38604, 38605, 38606,
       38607, 38741, 38742,
       41025, 46605, 46606)
order by account_id
