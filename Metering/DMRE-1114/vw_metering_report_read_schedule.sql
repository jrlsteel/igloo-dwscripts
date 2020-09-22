create or replace view vw_metering_report_read_schedule as
    with cte_read_permissions as (
        select *,
               row_number() over (partition by entity_id order by effective_from desc) as rn
        from ref_cdb_attributes
        where attribute_type_id = 23
          and getdate() between nvl(effective_from, getdate() - 1) and nvl(effective_to, getdate() + 1)
    )
    select scoa.id as sc_id,
           scoa.external_id,
           up.user_id,
           attr_rp.entity_type,
           attr_rp.attribute_type_id,
           attr_rp.attribute_value_id,
           attrv.attribute_value
    from vw_supply_contracts_with_occ_accs scoa
             inner join ref_cdb_user_permissions up on scoa.id = up.permissionable_id and
                                                       up.permissionable_type = 'App\\SupplyContract' and
                                                       up.permission_level = 0
             inner join cte_read_permissions attr_rp on scoa.id = attr_rp.entity_id and attr_rp.rn = 1
             inner join ref_cdb_attribute_values attrv on attr_rp.attribute_value_id = attrv.id;

alter table vw_metering_report_read_schedule
    owner to igloo;


