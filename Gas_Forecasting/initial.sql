-- elec hh forecasting ignoring portfolio growth
select to_date(ppc.st_date, 'YYYYMMDD')                           as date,
       sum(ind_eac.estimation_value)                              as eac,
       count(ind_eac.estimation_value)                            as num_eac,
       count(*)                                                   as num_all,
       sum(ppc_1 * ind_eac.estimation_value) * num_all / num_eac  as kwh_1,
       sum(ppc_2 * ind_eac.estimation_value) * num_all / num_eac  as kwh_2,
       sum(ppc_3 * ind_eac.estimation_value) * num_all / num_eac  as kwh_3,
       sum(ppc_4 * ind_eac.estimation_value) * num_all / num_eac  as kwh_4,
       sum(ppc_5 * ind_eac.estimation_value) * num_all / num_eac  as kwh_5,
       sum(ppc_6 * ind_eac.estimation_value) * num_all / num_eac  as kwh_6,
       sum(ppc_7 * ind_eac.estimation_value) * num_all / num_eac  as kwh_7,
       sum(ppc_8 * ind_eac.estimation_value) * num_all / num_eac  as kwh_8,
       sum(ppc_9 * ind_eac.estimation_value) * num_all / num_eac  as kwh_9,
       sum(ppc_10 * ind_eac.estimation_value) * num_all / num_eac as kwh_10,
       sum(ppc_11 * ind_eac.estimation_value) * num_all / num_eac as kwh_11,
       sum(ppc_12 * ind_eac.estimation_value) * num_all / num_eac as kwh_12,
       sum(ppc_13 * ind_eac.estimation_value) * num_all / num_eac as kwh_13,
       sum(ppc_14 * ind_eac.estimation_value) * num_all / num_eac as kwh_14,
       sum(ppc_15 * ind_eac.estimation_value) * num_all / num_eac as kwh_15,
       sum(ppc_16 * ind_eac.estimation_value) * num_all / num_eac as kwh_16,
       sum(ppc_17 * ind_eac.estimation_value) * num_all / num_eac as kwh_17,
       sum(ppc_18 * ind_eac.estimation_value) * num_all / num_eac as kwh_18,
       sum(ppc_19 * ind_eac.estimation_value) * num_all / num_eac as kwh_19,
       sum(ppc_20 * ind_eac.estimation_value) * num_all / num_eac as kwh_20,
       sum(ppc_21 * ind_eac.estimation_value) * num_all / num_eac as kwh_21,
       sum(ppc_22 * ind_eac.estimation_value) * num_all / num_eac as kwh_22,
       sum(ppc_23 * ind_eac.estimation_value) * num_all / num_eac as kwh_23,
       sum(ppc_24 * ind_eac.estimation_value) * num_all / num_eac as kwh_24,
       sum(ppc_25 * ind_eac.estimation_value) * num_all / num_eac as kwh_25,
       sum(ppc_26 * ind_eac.estimation_value) * num_all / num_eac as kwh_26,
       sum(ppc_27 * ind_eac.estimation_value) * num_all / num_eac as kwh_27,
       sum(ppc_28 * ind_eac.estimation_value) * num_all / num_eac as kwh_28,
       sum(ppc_29 * ind_eac.estimation_value) * num_all / num_eac as kwh_29,
       sum(ppc_30 * ind_eac.estimation_value) * num_all / num_eac as kwh_30,
       sum(ppc_31 * ind_eac.estimation_value) * num_all / num_eac as kwh_31,
       sum(ppc_32 * ind_eac.estimation_value) * num_all / num_eac as kwh_32,
       sum(ppc_33 * ind_eac.estimation_value) * num_all / num_eac as kwh_33,
       sum(ppc_34 * ind_eac.estimation_value) * num_all / num_eac as kwh_34,
       sum(ppc_35 * ind_eac.estimation_value) * num_all / num_eac as kwh_35,
       sum(ppc_36 * ind_eac.estimation_value) * num_all / num_eac as kwh_36,
       sum(ppc_37 * ind_eac.estimation_value) * num_all / num_eac as kwh_37,
       sum(ppc_38 * ind_eac.estimation_value) * num_all / num_eac as kwh_38,
       sum(ppc_39 * ind_eac.estimation_value) * num_all / num_eac as kwh_39,
       sum(ppc_40 * ind_eac.estimation_value) * num_all / num_eac as kwh_40,
       sum(ppc_41 * ind_eac.estimation_value) * num_all / num_eac as kwh_41,
       sum(ppc_42 * ind_eac.estimation_value) * num_all / num_eac as kwh_42,
       sum(ppc_43 * ind_eac.estimation_value) * num_all / num_eac as kwh_43,
       sum(ppc_44 * ind_eac.estimation_value) * num_all / num_eac as kwh_44,
       sum(ppc_45 * ind_eac.estimation_value) * num_all / num_eac as kwh_45,
       sum(ppc_46 * ind_eac.estimation_value) * num_all / num_eac as kwh_46,
       sum(ppc_47 * ind_eac.estimation_value) * num_all / num_eac as kwh_47,
       sum(ppc_48 * ind_eac.estimation_value) * num_all / num_eac as kwh_48,
       sum(ppc_49 * ind_eac.estimation_value) * num_all / num_eac as kwh_49,
       sum(ppc_50 * ind_eac.estimation_value) * num_all / num_eac as kwh_50

from ref_meterpoints mp_elec
         left join ref_meterpoints_attributes rma_gsp
                   on mp_elec.account_id = rma_gsp.account_id and mp_elec.meter_point_id = rma_gsp.meter_point_id and
                      rma_gsp.attributes_attributename = 'GSP'
         left join ref_meterpoints_attributes rma_ssc
                   on mp_elec.account_id = rma_ssc.account_id and mp_elec.meter_point_id = rma_ssc.meter_point_id and
                      rma_ssc.attributes_attributename = 'SSC'
         left join ref_meterpoints_attributes rma_pcl
                   on mp_elec.account_id = rma_pcl.account_id and mp_elec.meter_point_id = rma_pcl.meter_point_id and
                      rma_pcl.attributes_attributename = 'Profile Class'
         left join ref_meters mtrs_elec
                   on mtrs_elec.account_id = mp_elec.account_id and
                      mtrs_elec.meter_point_id = mp_elec.meter_point_id and mtrs_elec.removeddate is NULL
         left join ref_registers reg_elec
                   on reg_elec.account_id = mp_elec.account_id and mtrs_elec.meter_id = reg_elec.meter_id
         left join (select *,
                           row_number()
                           over (partition by account_id,
                               mpan,
                               register_id,
                               serial_number
                               order by effective_from desc) as rn
                    from ref_estimates_elec_internal) ind_eac
                   on ind_eac.rn = 1 and
                      mp_elec.account_id = ind_eac.account_id and
                      mp_elec.meterpointnumber = ind_eac.mpan and
                      reg_elec.registers_registerreference = ind_eac.register_id and
                      mtrs_elec.meterserialnumber = ind_eac.serial_number
         left join ref_d18_ppc_forecast ppc
                   on ppc.gsp_group_id = rma_gsp.attributes_attributevalue and
                      ppc.pcl_id::int = rma_pcl.attributes_attributevalue::int and
                      ppc.ss_conf_id::int = rma_ssc.attributes_attributevalue::int and
                      ppc.time_pattern_regime::int = reg_elec.registers_tpr::int and
                      to_date(ppc.st_date, 'YYYYMMDD') between greatest(associationstartdate, supplystartdate) and
                          least(associationenddate, supplyenddate, getdate() + 1000)
where mp_elec.meterpointtype = 'E'
  and date >= trunc(getdate())
group by st_date
order by st_date

-- gas hh no growth
select to_date(alp.date, 'YYYY-MM-DD')                                                            as date,
       sum(ind_aq.estimation_value)                                                               as aq,
       count(ind_aq.estimation_value)                                                             as num_aq,
       count(*)                                                                                   as num_all,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_1,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_2,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_3,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_4,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_5,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_6,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_7,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_8,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_9,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_10,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_11,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_12,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_13,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_14,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_15,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_16,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_17,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_18,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_19,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_20,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_21,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_22,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_23,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_48,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_25,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_26,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_27,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_28,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_29,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_30,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_31,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_32,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_33,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_34,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_35,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_36,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_37,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_38,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_39,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_40,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_41,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_42,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_43,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_44,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_45,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_46,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_47,
       sum(ind_aq.estimation_value * ((alp.forecastdocumentation / 48) / 365)) * num_all / num_aq as kwh_48,
       null                                                                                       as kwh_49,
       null                                                                                       as kwh_50

from ref_meterpoints mp_gas
         left join ref_meterpoints_attributes rma_ldz
                   on mp_gas.account_id = rma_ldz.account_id and mp_gas.meter_point_id = rma_ldz.meter_point_id and
                      rma_ldz.attributes_attributename = 'LDZ'
         left join ref_meters mtrs_gas
                   on mtrs_gas.account_id = mp_gas.account_id and
                      mtrs_gas.meter_point_id = mp_gas.meter_point_id and mtrs_gas.removeddate is NULL
         left join ref_registers reg_gas
                   on reg_gas.account_id = mp_gas.account_id and mtrs_gas.meter_id = reg_gas.meter_id
         left join (select *,
                           row_number()
                           over (partition by account_id,
                               mprn,
                               register_id,
                               serial_number
                               order by effective_from desc) as rn
                    from ref_estimates_gas_internal) ind_aq
                   on ind_aq.rn = 1 and
                      mp_gas.account_id = ind_aq.account_id and
                      mp_gas.meterpointnumber = ind_aq.mprn and
                      reg_gas.registers_registerreference = ind_aq.register_id and
                      mtrs_gas.meterserialnumber = ind_aq.serial_number
         left join ref_alp_igloo_daf_wcf alp
                   on alp.ldz = rma_ldz.attributes_attributevalue and
                      to_date(alp.date, 'YYYY-MM-DD') between greatest(associationstartdate, supplystartdate) and
                          least(associationenddate, supplyenddate, getdate() + 1000)
where mp_gas.meterpointtype = 'G'
  and date >= trunc(getdate())
group by date
order by date



-- predicting future account numbers (work postponed)
with live_acc_nums as (select coalesce(ssd_date, sed_date)                                as date,
                              nvl(num_ssd, 0)                                             as ssd,
                              nvl(num_sed, 0)                                             as sed,
                              ssd - sed                                                   as increase,
                              sum(increase) over (order by date rows unbounded preceding) as num_live,
                              sum(increase) over (order by date rows 6 preceding) / 7     as daily_growth_7,
                              sum(increase) over (order by date rows 29 preceding) / 30   as daily_growth_30,
                              sum(increase) over (order by date rows 182 preceding) / 183 as daily_growth_183
                       from (select acc_ssd as ssd_date, count(*) as num_ssd
                             from ref_calculated_daily_customer_file
                             where acc_ssd is not null
                             group by acc_ssd) ssd
                                full join
                            (select acc_ed as sed_date, count(*) as num_sed
                             from ref_calculated_daily_customer_file
                             where acc_ed is not null
                             group by acc_ed) sed
                            on ssd.ssd_date = sed.sed_date
                       order by date)
select num_live as current_num_live,
       daily_growth_7,
       daily_growth_30,
       daily_growth_183
from live_acc_nums lan_today
where trunc(lan_today.date) = trunc(getdate())


-- live accounts
