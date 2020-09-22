create or replace view vw_metering_report_psr as
select scoa.id,
       scoa.external_id,
       up.user_id,
       attr_psr.entity_type,
       attr_psr.attribute_type_id,
       attr_psr.attribute_value_id,
       regexp_replace(attr_psr.attribute_custom_value, '[^0-9\,]') as psr -- remove characters other than digits and commas

from vw_supply_contracts_with_occ_accs scoa
         inner join ref_cdb_user_permissions up
                    on scoa.id = up.permissionable_id and up.permissionable_type = 'App\\SupplyContract'
         inner join (
    select *, row_number() over (partition by entity_id order by updated_at desc) as rn
    from ref_cdb_attributes
    where attribute_type_id = 17
      and getdate() between nvl(effective_from, getdate() - 1) and
        nvl(effective_to, getdate() + 1)
) attr_psr on scoa.id = attr_psr.entity_id and attr_psr.rn = 1;

alter table vw_metering_report_psr
    owner to igloo;