truncate table ref_sme_accounts;
insert into ref_sme_accounts
select account_id,
       eligibility                                                                          as eligibility_status,
       reason                                                                               as detailed_status,
       case when left(reason, 7) in ('eligibl', 'pending') then eligible_from else null end as eligible_from,
       getdate()                                                                            as etlchange
from (select cf.account_id,
             case
                 when region_open is not null then greatest(ssd, region_open, override_end)
                 else null end      as eligible_from,
             case
                 when account_status in ('Final', 'Cancelled') then 'acc_closed'
                 when account_status = 'Pending Final' then 'acc_closing'
                 when missing_meter_details then 'pending_meter_details'
                 when num_elec_reg > 1 then 'too_many_elec'
                 when num_elec_reg < 1 then 'no_elec'
                 when num_gas_reg > 1 then 'too_many_gas'
                 when region_open is null then 'region_not_scheduled'
                 when override_active and override_end is null then 'override_indefinite'
                 when eligible_from > getdate() then
                     case
                         when eligible_from = ssd then 'pending_ssd'
                         when eligible_from = override_end then 'pending_override'
                         else 'pending_region'
                         end
                 else 'eligible'
                 end                as reason,
             case
                 when reason in ('eligible') then 'eligible'
                 when reason in
                      ('acc_closed', 'acc_closing', 'too_many_elec', 'no_elec', 'too_many_gas')
                     then 'ineligible'
                 else 'pending' end as eligibility
      from vw_customer_file cf
               left join
           (select account_id,
                   sum(case when num_reg = 0 then 1 else 0 end) > 0            as missing_meter_details,
                   sum(case when meterpointtype = 'E' then num_reg else 0 end) as num_elec_reg,
                   sum(case when meterpointtype = 'G' then num_reg else 0 end) as num_gas_reg,
                   min(ssd)                                                    as ssd
            from (select mp.account_id,
                         mp.meter_point_id,
                         count(reg.register_id)                               as num_reg,
                         mp.meterpointtype,
                         min(greatest(associationstartdate, supplystartdate)) as ssd
                  from ref_meterpoints mp
                           left join ref_meters met
                                     on met.account_id = mp.account_id and met.meter_point_id = mp.meter_point_id and
                                        met.removeddate is null
                           left join ref_registers reg
                                     on reg.account_id = met.account_id and reg.meter_id = met.meter_id
                  where least(associationenddate, supplyenddate) is null
                     or least(associationenddate, supplyenddate) >= getdate()
                  group by mp.account_id, mp.meterpointtype, mp.meter_point_id
                 ) mp_sums
            group by account_id
           ) account_mp_status on cf.account_id = account_mp_status.account_id
               left join ref_cdb_supply_contracts sc on cf.account_id = sc.external_id
               left join ref_cdb_addresses addr on sc.supply_address_id = addr.id
               left join (select pcs.postcode_prefix,
                                 region_open,
                                 ovr.postcode_prefix is not null as override_active,
                                 override_end
                          from (select map.postcode_prefix,
                                       min(zones.open_date) as region_open
                                from ref_sme_postcode_mapping map
                                         left join ref_sme_zones zones on map.zone_id = zones.zone_id
                                group by map.postcode_prefix) pcs
                                   left join ref_sme_postcode_overrides ovr
                                             on pcs.postcode_prefix = ovr.postcode_prefix and
                                                ovr.override_start <= getdate() and
                                                (ovr.override_end is null or ovr.override_end >= getdate())) postcode_status
                         on left(addr.postcode, len(addr.postcode) - 3) = postcode_status.postcode_prefix
     ) acc_es
order by account_id;