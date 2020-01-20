/*
 1) Check the comparison sql code returns nothing when comparing a table to itself
    (by definition, no changes should be found)
 */
select *
from temp_tado_diffs_identical;
select *
from temp_tado_audit_identical;

/*
 2) Check no rows have been added or removed from the tado results
 */
select *
from temp_tado_audit
where etlchangetype in ('n', 'r');

/*
 3) Check only one row is present for each key triple
 */
select user_id, account_id, supply_address_id, count(*)
from temp_tado_new
group by user_id, account_id, supply_address_id
having count(*) > 1;

/*
 4) Check the new tado SQL code only returns expected differences to the old sql, e.g. no longer returning non-age
    attributes in the age column. Particular care should be taken in checking segment & savings categories
 */
select field, count(*)
from temp_tado_diffs ttd
         left join ref_calculated_daily_customer_file dcf on ttd.account_id = dcf.account_id
where dcf.account_status not in ('Cancelled', 'Final')
group by field
order by count(*) desc;

/*
 5) Check the age column only contains age attributes
 */
select attr_ages.attribute_custom_value, *, left(ages, 6) as ages_start
from ref_calculated_tado_efficiency_batch teb
         left join ref_cdb_attributes attr_ages on attr_ages.attribute_type_id = 1 and attr_ages.entity_id = teb.user_id
where ages_start not in ('[{"age', '[]', 'unknow')


-- 4.xx

select user_id,
       ttd.account_id,
       supply_address_id,
       field,
       old_val,
       new_val,
       new_val::double precision - old_val::double precision as difference,
       abs(difference / old_val::double precision)           as prop_dif
from temp_tado_diffs ttd
         left join ref_calculated_daily_customer_file dcf on ttd.account_id = dcf.account_id
where field = 'annual_consumption'
  and old_val is not null
  and dcf.gas_reg_status not in ('Final', 'Cancelled')
order by prop_dif desc


-- for numeric fields
with live_cust_diffs as (select ttd.*
                         from temp_tado_diffs ttd
                                  left join ref_calculated_daily_customer_file dcf on ttd.account_id = dcf.account_id
                         where dcf.account_status not in ('Final', 'Cancelled'))
select user_id,
       account_id,
       supply_address_id,
       old_val,
       new_val,
       abs((new_val::double precision - old_val::double precision) / old_val::double precision) as perc_diff
from live_cust_diffs
where field = 'savings_perc'
  and old_val is not null
order by perc_diff desc

-- for discrete fields
with live_cust_diffs as (select ttd.*
                         from temp_tado_diffs ttd
                                  left join ref_calculated_daily_customer_file dcf on ttd.account_id = dcf.account_id
                         where dcf.account_status not in ('Final', 'Cancelled'))
select old_val,
       new_val,
       count(*)
from live_cust_diffs
where field = 'segment'
group by old_val, new_val
order by count(*) desc

with live_cust_diffs as (select ttd.*
                         from temp_tado_diffs ttd
                                  left join ref_calculated_daily_customer_file dcf on ttd.account_id = dcf.account_id
                         where dcf.account_status not in ('Final', 'Cancelled'))
select lcd.account_id,
       lcd.old_val,
       lcd.new_val,
       tto.savings_in_pounds,
       ttn.savings_in_pounds,
       tto.savings_perc_source,
       ttn.savings_perc_source,
       tto.savings_perc,
       ttn.savings_perc,
       ttn.annual_consumption,
       ttn.unit_rate_with_vat,
       ttn.fuel_type,
       ttn.heating_source
from live_cust_diffs lcd
         left join temp_tado_old tto on lcd.account_id = tto.account_id
         left join temp_tado_new ttn on lcd.account_id = ttn.account_id
where field = 'segment'

select *
from ref_cdb_attributes
where attribute_type_id = 22

