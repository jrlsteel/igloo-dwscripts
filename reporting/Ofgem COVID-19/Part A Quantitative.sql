select dcf.account_id,
       up.user_id,
       trunc(date_trunc('week', getdate()) - 1)                                                 as date_of_sunday,
       trunc(dcf.acc_ssd) <= date_of_sunday and trunc(nvl(acc_ed, getdate())) >= date_of_sunday as acc_live,
       trunc(dcf.latest_bill_date) between (date_of_sunday - 6) and date_of_sunday              as bill_in_window,
       attr_psr.attribute_custom_value is not null                                              as psr,
       ((num_elec_mpns * vlr.elec_sc * 365) + (eac_igloo_ca * vlr.elec_ur)) * 0.01 * 1.05 /
       12                                                                                       as elec_monthly_usage_sterling,
       ((num_gas_mpns * vlr.gas_sc * 365) + (aq_igloo_ca * vlr.gas_ur)) * 0.01 * 1.05 /
       12                                                                                       as gas_monthly_usage_sterling,
       nvl(elec_monthly_usage_sterling, 0) +
       nvl(gas_monthly_usage_sterling, 0)                                                       as monthly_usage_sterling
from ref_calculated_daily_customer_file dcf
         left join ref_cdb_supply_contracts sc on dcf.account_id = sc.external_id
         left join ref_cdb_user_permissions up on up.permissionable_type = 'App\\SupplyContract' and
                                                  up.permissionable_id = sc.id and
                                                  up.permission_level = 0
         left join ref_cdb_attributes attr_psr on attr_psr.attribute_type_id = 17 and
                                                  attr_psr.attribute_custom_value != '[]' and
                                                  attr_psr.effective_to is null and
                                                  attr_psr.entity_id = sc.id
         left join vw_latest_rates_ensek vlr on vlr.account_id = dcf.account_id
