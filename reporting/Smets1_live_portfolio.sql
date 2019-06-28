select distinct
    stats.account_id,
    --mp.meter_point_id,
    --stats.acc_stat,
    mp.meterpointnumber as meterpoint_number,
    mp.meterpointtype as elec_or_gas,
    elec_make_model.attributes_attributevalue as elec_make_model,
    gas_man_code.metersattributes_attributevalue as gas_manufacturer_code,
    gas_model.metersattributes_attributevalue as gas_model
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
            case when associationstartdate > supplystartdate
              then 1 else 0 end                                         as hmi
        from ref_meterpoints
        where (start_date < end_date or end_date isnull) --non-cancelled meterpoints only
            --exclude known erroneous accounts
            and account_id not in (29678,36991,38044,38114,38601,38602,38603,38604,38605,38606,38607,38741,38742,41025,43866,45731,46091,46605,46606)
    ) mp_stat
    group by mp_stat.account_id
    order by mp_stat.account_id
) stats
left outer join ref_meterpoints mp on stats.account_id = mp.account_id
                                          and mp.associationenddate isnull
                                          and mp.supplyenddate isnull
left outer join ref_meterpoints_attributes elec_make_model
    on elec_make_model.meter_point_id = mp.meter_point_id
           and elec_make_model.attributes_attributename = 'MeterMakeAndModel'
left outer join ref_meters_attributes gas_man_code
    on gas_man_code.meter_point_id = mp.meter_point_id
           and gas_man_code.metersattributes_attributename='Meter_Manufacturer_Code'
left outer join ref_meters_attributes gas_model
    on gas_model.meter_point_id = mp.meter_point_id
           and gas_model.metersattributes_attributename='Model_Code'
where stats.acc_stat in ('Live')
order by account_id asc