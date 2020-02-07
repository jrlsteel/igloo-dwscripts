/*drop table if exists temp_pin_old;
create table temp_pin_old as*/
/*drop table if exists temp_pin_update;
create table temp_pin_update as
*/
select UPDATEDEVICECONFIG,
       DEVICETYPE,
       MPXN,
       EXPORTMPAN,
       SECONDARYIMPORTMPAN,
       TARIFF,
       DEFAULTCONFIGURATION,
       APPLYPREPAYMENTCONFIGURATION,
       PAYMENTMODE,
       null                                                          as PREPAYMENTCONFIG,
       null                                                          as DEBTCONFIG,
       READCYCLEDAILY,
       READCYCLEDAILYFREQUENCY,
       READCYCLEHALFHOURLY,
       READCYCLEHALFHOURLYFREQUENCY,
       to_char(BILLINGDATALOGSTART, 'YYYY-MM-DD') || 'T00:00:00.05Z' as BILLINGDATALOGSTART,
       BILLINGDATALOGFREQUENCY,
       null                                                          as PAN,
       null                                                          as COMPANYNAME,
       null                                                          as COMPANYCONTRACTID,
       null                                                          as LDZ,
       null                                                          as TOPUPAMOUNT,
       null                                                          as ACTIVATEEMERGENCYCREDIT,
       null                                                          as PREPAYDAILYREADLOG,
       null                                                          as PREPAYDAILYREADLOGFREQUENCY,
       LDZ                                                           as LDZ
from (select distinct max(tr.sourcedate::timestamp)
                      over (partition by tr.account_id::int)                       as dd_date,
                      trunc(ba.start::timestamp)                                   as install_date,
                      left(max(rmp.meterpointnumber) over (partition by sc.external_id),
                           2)                                                      as region_code,
                      1                                                            as UPDATEDEVICECONFIG,
                      case when len(rmp.meterpointnumber) > 10 then 0 else 1 end   as DEVICETYPE,
                      rmp.meterpointnumber                                         as MPXN,
                      null                                                         as EXPORTMPAN,
                      null                                                         as SECONDARYIMPORTMPAN,
                      CASE account_gsp.gsp
                          WHEN '_A' THEN '10'
                          WHEN '_B' THEN '11'
                          WHEN '_C' THEN '12'
                          WHEN '_D' THEN '13'
                          WHEN '_E' THEN '14'
                          WHEN '_F' THEN '15'
                          WHEN '_G' THEN '16'
                          WHEN '_P' THEN '17'
                          WHEN '_N' THEN '18'
                          WHEN '_J' THEN '19'
                          WHEN '_H' THEN '20'
                          WHEN '_K' THEN '21'
                          WHEN '_L' THEN '22'
                          WHEN '_M' THEN '23'
                          END                                                      as region_code,
                      case DEVICETYPE
                          when 0 then 'ELECPION' || region_code
                          when 1 then 'GASPION' || region_code
                          else null end                                            as TARIFF,
                      case DEVICETYPE
                          when 0 then 'ACCELERO_DEFAULT_ESME_001'
                          when 1 then 'ACCELERO_DEFAULT_GSME_001'
                          else null end                                            as DEFAULTCONFIGURATION,
                      1                                                            as APPLYPREPAYMENTCONFIGURATION,
                      'Credit'                                                     as PAYMENTMODE,
                      1                                                            as READCYCLEDAILY,
                      'Daily'                                                      as READCYCLEDAILYFREQUENCY,
                      (nvl(consent_type.attribute_value, '') = 'half_hourly')::int as READCYCLEHALFHOURLY,
                      'Daily'                                                      as READCYCLEHALFHOURLYFREQUENCY,
                      add_months(dd_date,
                                 datediff(months,
                                          date_trunc('months', dd_date),
                                          date_trunc('months', install_date)) +
                                 case
                                     when date_part(days, dd_date) > date_part(days, install_date) then 0
                                     else 1 end)                                   as BILLINGDATALOGSTART,
                      'Monthly'                                                    as BILLINGDATALOGFREQUENCY,
                      nvl(dcf.ldz, most_common_ldz.ldz, 'SO')                      as LDZ
      from aws_s3_stage2_extracts.stage2_cdbbookingappointments ba
               inner join aws_s3_stage2_extracts.stage2_cdbbookingtypes bt
                          on ba.booking_type_id = bt.id and bt.slug = 'smart-install'
               inner join ref_cdb_user_permissions up
                          on up.user_id = ba.user_id::int and up.permissionable_type ilike 'App%SupplyContract' and
                             permission_level = 0
               inner join ref_cdb_supply_contracts sc on sc.id = up.permissionable_id
               inner join ref_cdb_registrations r
                          on r.supply_address_id = ba.address_id::int and sc.registration_id = r.id
               inner join ref_meterpoints rmp
                          on rmp.account_id = sc.external_id and
                             ba.created_at::timestamp between rmp.supplystartdate and -- booking appointment created while account was live
                                 nvl(greatest(rmp.supplyenddate, rmp.associationenddate), getdate() + 1)
               inner join ref_meters rm on rm.account_id = rmp.account_id and rm.meter_point_id = rmp.meter_point_id and
                                           rm.removeddate is null
               left join ref_account_transactions tr
                         on tr.account_id = sc.external_id and tr.method = 'Direct Debit' and
                            tr.transactiontype = 'PAYMENT' and tr.sourcedate <= ba.start::timestamp
               left join ref_cdb_attributes consent_id
                         on consent_id.attribute_type_id = 23 and consent_id.entity_id = sc.id and
                            consent_id.effective_to is null
               left join ref_cdb_attribute_values consent_type
                         on consent_type.attribute_type_id = 23 and consent_type.id = consent_id.attribute_value_id
               left join (select account_id, max(attributes_attributevalue) as gsp
                          from ref_meterpoints_attributes
                          where attributes_attributename = 'GSP'
                          group by account_id) account_gsp
                         on rmp.account_id = account_gsp.account_id
               left join ref_calculated_daily_customer_file dcf on sc.external_id = dcf.account_id
               left join (select outcode, ldz
                          from (select outcode,
                                       ldz,
                                       row_number() over (partition by outcode order by num_in_outcode desc) as rn
                                from (select outcode, ldz, count(*) as num_in_outcode
                                      from ref_calculated_daily_customer_file
                                      where ldz is not null
                                      group by ldz, outcode) ldz_freqs
                               ) ranked_ldz_per_outcode
                          where rn = 1) most_common_ldz on dcf.outcode = most_common_ldz.outcode

      where left(ba.status, 9) != 'cancelled' and $__timeFilter(ba.created_at)) calc

---- TESTING #####################

