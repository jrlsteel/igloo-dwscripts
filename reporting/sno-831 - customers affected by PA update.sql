select sc.external_id                                                                          as account_id,
       ind_aq,
       pa_cons_gas                                                                             as ann_cons_gas,
       get_best_consumption(igl_ind_aq, ind_aq, pa_cons_gas, quotes_aq, 'gas')                 as ca_gas_source,
       case ca_gas_source
           when 'igl_ind_aq' then igl_ind_aq
           when 'ind_aq' then ind_aq
           when 'pa_cons_gas' then pa_cons_gas
           when 'quotes_aq' then quotes_aq
           end                                                                                 as ca_gas_val,
       ind_aq * 1.0 / (ann_cons_gas + 0.001)                                                   as old_gas_dif,
       ca_gas_val * 1.0 / (ann_cons_gas + 0.001)                                               as new_gas_dif,
       ind_eac,
       pa_cons_elec                                                                            as ann_cons_elec,
       get_best_consumption(igl_ind_eac, ind_eac, pa_cons_elec, quotes_eac, 'elec')            as ca_elec_source,
       case ca_elec_source
           when 'igl_ind_eac' then igl_ind_eac
           when 'ind_eac' then ind_eac
           when 'pa_cons_elec' then pa_cons_elec
           when 'quotes_eac' then quotes_eac
           end                                                                                 as ca_elec_val,
       ind_eac * 1.0 / (ann_cons_elec + 0.001)                                                 as old_elec_dif,
       ca_elec_val * 1.0 / (ann_cons_elec + 0.001)                                             as new_elec_dif,
       (least(old_elec_dif, old_gas_dif) <= 0.7 or greatest(old_elec_dif, old_gas_dif) >= 1.3) as fail_old_pa,
       (least(new_elec_dif, new_gas_dif) <= 0.7 or greatest(new_elec_dif, new_gas_dif) >= 1.3) as fail_new_pa,
       balances.currentbalance                                                                 as account_balance,
       dds.amount                                                                              as direct_debit,
       account_balance / direct_debit                                                          as balance_over_dd,
       u.first_name,
       u.last_name,
       u.email,
       ssd_sed.ssd,
       ssd_sed.sed,

from ref_cdb_supply_contracts sc
         left join ref_consumption_accuracy_gas rcag on rcag.account_id = sc.external_id
         left join ref_consumption_accuracy_elec rcae on rcae.account_id = sc.external_id
         left join (select account_id, currentbalance
                    from (select *, row_number() over (partition by account_id order by sourcedate desc) as rn
                          from ref_account_transactions) most_recent_transaction
                    where rn = 1) balances on sc.external_id = balances.account_id
         left join (select account_id, amount
                    from (select *, row_number() over (partition by account_id order by sourcedate desc) as rn
                          from ref_account_transactions
                          where method = 'Direct Debit'
                            and transactiontype = 'PAYMENT') most_recent_dd
                    where rn = 1) dds on sc.external_id = dds.account_id
         left join ref_cdb_user_permissions up
                   on up.permissionable_type ilike 'App%SupplyContract' and up.permissionable_id = sc.id
         left join ref_cdb_users u on up.user_id = u.id
         left join (select account_id,
                           min(greatest(supplystartdate, associationstartdate))                                   as ssd,
                           nullif(max(nvl(least(supplyenddate, associationenddate)), '2100-01-01'), '2100-01-01') as sed
                    from ref_meterpoints
                    group by account_id) ssd_sed on ssd_sed.account_id = sc.external_id
         left join (select account_id, max(meterreadingdatetime) as most_recent_reading
                    from ref_readings_internal_valid
                    group by account_id) latest_reading
;