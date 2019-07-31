select coalesce(elec_stats.account_id, gas_stats.account_id)                            as account_id,
       elec_stats.start_date                                                            as Elec_SSD,
       gas_stats.start_date                                                             as Gas_SSD,
       elec_stats.end_date                                                              as Elec_ED,
       gas_stats.end_date                                                               as Gas_ED,
       elec_stats.num_meterpoints                                                       as num_elec_MPNs,
       coalesce(gas_stats.num_meterpoints, 0)                                           as num_gas_MPNs,
       nullif(cons_acc_elec.ind_eac, 0)                                                 as EAC_industry,
       nullif(cons_acc_gas.ind_aq, 0)                                                   as AQ_industry,
       nullif(cons_acc_elec.igl_ind_eac, 0)                                             as EAC_Igloo,
       nullif(cons_acc_gas.igl_ind_aq, 0)                                               as AQ_Igloo,
       case
           when dd_pay_layers.wu_app_current then dd_pay_layers.dd_total
           else dd_pay_layers.dd_total - dd_pay_layers.wu_amount end                    as reg_pay_amount,
       dd_pay_layers.dd_total - dd_pay_layers.wu_amount                                 as reg_pay_amount_ex_wu,
       dd_pay_layers.wu_amount                                                          as wu_amount,
       dd_pay_layers.wu_amount > 0                                                      as wu_this_year,
       most_recent_dd.payment_day                                                       as reg_pay_date,
       q.projected_savings                                                              as projected_savings,
       most_recent_transaction.currentbalance                                           as balance,
       ((365 * lr.elec_sc) + (cons_acc_elec.ind_eac * lr.elec_ur)) / 100                as elec_annual_spend,
       ((365 * lr.gas_sc) + (cons_acc_gas.ind_aq * lr.gas_ur)) / 100                    as gas_annual_spend,
       elec_stats.reg_status                                                            as elec_reg_status,
       gas_stats.reg_status                                                             as gas_reg_status,
       case when elec_stats.reg_status = 'Final' then elec_stats.losstype else null end as elec_loss_type,
       case when gas_stats.reg_status = 'Final' then gas_stats.losstype else null end   as gas_loss_type,
       case
           when elec_reg_status is null then 'Gas'
           when gas_reg_status is null then 'Elec'
           else 'Dual'
           end                                                                          as supply_type,
       udf_meterpoint_status(
               least(Gas_SSD, Elec_SSD),
               case supply_type
                   when 'Gas' then Gas_ED
                   when 'Elec' then Elec_ED
                   else nulls_latest(Gas_ED, Elec_ED)
                   end
           )                                                                            as account_status,
       case
           when account_status = 'Final' then
               case when greatest(Elec_ED,Gas_ED) = Elec_ED then elec_loss_type else gas_loss_type end
           end                                                                          as account_loss_type,

       elec_stats.GSP                                                                   as GSP,
       gas_stats.LDZ                                                                    as LDZ


from
    -- ELEC ------------------------------------------------------------------------------------------------ ELEC
    (select mp_elec.account_id,
            count(distinct meterpointnumber)                                as num_meterpoints,
            min(greatest(supplystartdate, associationstartdate))            as start_date,
            nullif(max(coalesce(least(supplyenddate, associationenddate),
                                current_date + 1000)), current_date + 1000) as end_date,
            --sum(igloo_eac.igl_ind_eac)                                      as igloo_EAC,
            --sum(industry_eac.estimation_value)                              as industry_EAC,
            udf_meterpoint_status(
                    min(greatest(supplystartdate, associationstartdate)),
                    nullif(max(coalesce(least(supplyenddate, associationenddate), current_date + 1000)),
                           current_date + 1000)
                )                                                           as reg_status,
            case
                when coalesce(max(mp_elec.associationenddate), current_date + 1000)
                    >= max(mp_elec.supplyenddate) then 'COS'
                else 'COT' end                                              as losstype,
            min(rma_gsp.attributes_attributevalue)                          as GSP
     from (select *,
                  max(least(supplyenddate, associationenddate))
                  over (partition by account_id, meterpointtype) as mp_end_date
           from ref_meterpoints
           where meterpointtype = 'E') mp_elec
              left join
          (select sum(igl_ind_eac) over (partition by account_id, meterpoint_id, trunc(etlchange))   as igl_ind_eac,
                  account_id,
                  meterpoint_id,
                  row_number() over (partition by account_id, meterpoint_id order by etlchange desc) as rn
           from ref_calculated_igl_ind_eac
           where meter_removed_date isnull) igloo_eac
          on igloo_eac.account_id = mp_elec.account_id and igloo_eac.meterpoint_id = mp_elec.meter_point_id and
             igloo_eac.rn = 1
              left join
          (select
               -- Sum of estimation values for a given account, mpn and day taken to cover all registers under that mpn
               sum(estimation_value) over (partition by account_id, mpan, effective_from)     as estimation_value,
               account_id,
               mpan,
               row_number() over (partition by account_id, mpan order by effective_from desc) as rn
           from ref_estimates_elec_internal
           where islive = true) industry_eac
          on industry_eac.account_id = mp_elec.account_id and industry_eac.mpan = mp_elec.meterpointnumber and
             industry_eac.rn = 1
              left join ref_meterpoints_attributes rma_gsp
                        on rma_gsp.meter_point_id = mp_elec.meter_point_id and rma_gsp.attributes_attributename = 'GSP'
     where coalesce(datediff(days, least(supplyenddate, associationenddate), mp_end_date), 0) <= 31
       and (greatest(supplystartdate, associationstartdate) < least(supplyenddate, associationenddate)
         or (supplyenddate isnull and associationenddate isnull)) --non-cancelled meterpoints only
     group by mp_elec.account_id) elec_stats
        left join ref_consumption_accuracy_elec cons_acc_elec on elec_stats.account_id = cons_acc_elec.account_id
        -- ELEC ------------------------------------------------------------------------------------------------ ELEC
        full join
    -- GAS ---------------------------------------------------------------------------------------------- GAS
        (select mp_gas.account_id,
                count(distinct meterpointnumber)                                as num_meterpoints,
                min(greatest(supplystartdate, associationstartdate))            as start_date,
                nullif(max(coalesce(least(supplyenddate, associationenddate),
                                    current_date + 1000)), current_date + 1000) as end_date,
                --sum(igloo_aq.igl_ind_aq)                                        as igloo_AQ,
                --sum(industry_aq.estimation_value)                               as industry_AQ,
                udf_meterpoint_status(
                        min(greatest(supplystartdate, associationstartdate)),
                        nullif(max(coalesce(least(supplyenddate, associationenddate), current_date + 1000)),
                               current_date + 1000)
                    )                                                           as reg_status,
                case
                    when coalesce(max(mp_gas.associationenddate), current_date + 1000)
                        >= max(mp_gas.supplyenddate) then 'COS'
                    else 'COT' end                                              as losstype,
                max(mp_gas.meter_point_id)                                      as gas_mpid,

                min(rma_ldz.attributes_attributevalue)                          as LDZ
         from (select *,
                      max(least(supplyenddate, associationenddate))
                      over (partition by account_id, meterpointtype) as mp_end_date
               from ref_meterpoints
               where meterpointtype = 'G') mp_gas
                  left join
              (select igl_ind_aq,
                      account_id,
                      meterpoint_id,
                      row_number() over (partition by account_id, register_id order by etlchange desc) as rn
               from ref_calculated_igl_ind_aq
               where meter_removed_date isnull) igloo_aq
              on igloo_aq.account_id = mp_gas.account_id and igloo_aq.meterpoint_id = mp_gas.meter_point_id and
                 igloo_aq.rn = 1
                  left join
              (select estimation_value,
                      account_id,
                      mprn,
                      row_number() over (partition by account_id, mprn order by effective_from desc) as rn
               from ref_estimates_gas_internal
               where islive = true) industry_aq
              on industry_aq.account_id = mp_gas.account_id and industry_aq.mprn = mp_gas.meterpointnumber and
                 industry_aq.rn = 1

                  left join ref_meterpoints_attributes rma_ldz
                            on rma_ldz.meter_point_id = mp_gas.meter_point_id and
                               rma_ldz.attributes_attributename = 'LDZ'
         where coalesce(datediff(days, least(supplyenddate, associationenddate), mp_end_date), 0) <= 31
           and (greatest(supplystartdate, associationstartdate) < least(supplyenddate, associationenddate)
             or (supplyenddate isnull and associationenddate isnull)) --non-cancelled meterpoints only
         group by mp_gas.account_id) gas_stats
    on gas_stats.account_id = elec_stats.account_id
        left join ref_consumption_accuracy_gas cons_acc_gas on gas_stats.account_id = cons_acc_gas.account_id
        -- GAS ---------------------------------------------------------------------------------------------- GAS

        -- QUOTES and BILLING ------------------------------------------------------------------------ QUOTES and BILLING
        left join ref_cdb_supply_contracts sc on coalesce(elec_stats.account_id, gas_stats.account_id) = sc.external_id
        left join ref_cdb_registrations r on sc.registration_id = r.id
        left join ref_cdb_quotes q on q.id = r.quote_id
        left join vw_latest_rates lr on coalesce(elec_stats.account_id, gas_stats.account_id) = lr.account_id

        -- STAGE 2 EXTRACTS FOR DIRECT DEBIT & ACCOUNT BALANCE ---------------------------------------------------------
        -- most recent direct debit payment (for payment day of month)
        left join (select account_id :: int                       as account_id,
                          -(amount :: double precision)           as amount,
                          date_part(day, sourcedate :: timestamp) as payment_day
                   from (
                            select *,
                                   row_number() over (partition by account_id order by sourcedate desc) as dd_rn
                            from aws_s3_stage2_extracts.stage2_accounttransactions
                            where method = 'Direct Debit'
                              and transactiontype = 'PAYMENT'
                        ) dd_with_rn
                   where dd_rn = 1) most_recent_dd
                  on most_recent_dd.account_id = coalesce(elec_stats.account_id, gas_stats.account_id)
        -- most recent transaction (for account balance)
        left join (select account_id :: int as account_id,
                          currentbalance :: double precision
                   from (
                            select *,
                                   row_number()
                                   over (partition by account_id order by creationdetail_createddate desc) as rn
                            from aws_s3_stage2_extracts.stage2_accounttransactions
                        ) dd_with_rn
                   where rn = 1) most_recent_transaction
                  on most_recent_transaction.account_id = coalesce(elec_stats.account_id, gas_stats.account_id)
        -- payment layers for upcoming dd amount (with and without winter uplift)
        left join (select acc_end_dates.account_id,
                          sum(case
                                  when pl.payment_type_id :: integer = 5 then pl.amount :: double precision
                                  else 0 end)                as wu_amount,
                          sum(case
                                  when pl.payment_type_id :: integer = 5 and
                                       pl.effective_from :: timestamp <= current_date
                                      then 1
                                  else 0 end) > 0            as wu_app_current,
                          sum(pl.amount :: double precision) as dd_total
                   from (select account_id,
                                max(coalesce(least(associationenddate, supplyenddate), current_date)) end_date
                         from ref_meterpoints
                         group by account_id) acc_end_dates
                            inner join ref_cdb_supply_contracts sc on acc_end_dates.account_id = sc.external_id
                            inner join aws_s3_stage2_extracts.stage2_cdbpaymentlayers pl
                                       on sc.id = pl.supply_contract_id and
                                          (pl.effective_to isnull or
                                           (pl.effective_to :: timestamp) >= acc_end_dates.end_date)
                   group by acc_end_dates.account_id) dd_pay_layers
                  on dd_pay_layers.account_id = coalesce(elec_stats.account_id, gas_stats.account_id)
where coalesce(elec_stats.account_id, gas_stats.account_id) not in --exclude known erroneous accounts
      (29678, 36991, 38044, 38114, 38601, 38602, 38603, 38604, 38605, 38606, 38607, 38741, 38742,
       41025, 46605, 46606)
order by account_id