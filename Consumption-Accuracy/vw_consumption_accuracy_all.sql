create or replace view vw_cons_acc_elec_all as
select ca.account_id,
       ca.reading_datetime,
       coalesce(ao.ann_cons_override, ca.pa_cons_elec)                          as pa_cons_elec,
       coalesce(ao.igl_ind_override, ca.igl_ind_eac)                            as igl_ind_eac,
       coalesce(ao.ind_override, ca.ind_eac)                                    as ind_eac,
       coalesce(ao.quote_override, ca.quotes_eac)                               as quotes_eac,
       get_best_consumption(coalesce(ao.igl_ind_override, ca.igl_ind_eac),
                            coalesce(ao.ind_override, ca.ind_eac),
                            coalesce(ao.ann_cons_override, ca.pa_cons_elec),
                            coalesce(ao.quote_override, ca.quotes_eac), 'elec') as ca_source,
       case ca_source
           when 'pa_cons_elec' then coalesce(ao.ann_cons_override, ca.pa_cons_elec)
           when 'igl_ind_eac' then coalesce(ao.igl_ind_override, ca.igl_ind_eac)
           when 'ind_eac' then coalesce(ao.ind_override, ca.ind_eac)
           when 'quotes_eac' then coalesce(ao.quote_override, ca.quotes_eac)
           end                                                                  as ca_value,
       ca.etlchangetype != 'r'                                                  as live,
       ca.etlchange
from (select *
      from (select *, row_number() over (partition by account_id order by etlchange desc) as recency
            from ref_consumption_accuracy_elec_audit) ordered_updates
      where ordered_updates.recency = 1
     ) ca
         left join vw_cons_acc_account_overrides ao
                   on ca.account_id = ao.account_id and ao.meterpointtype = 'E'
order by account_id;


create or replace view vw_cons_acc_gas_all as
select ca.account_id,
       ca.reading_datetime,
       coalesce(ao.ann_cons_override, ca.pa_cons_gas)                          as pa_cons_gas,
       coalesce(ao.igl_ind_override, ca.igl_ind_aq)                            as igl_ind_aq,
       coalesce(ao.ind_override, ca.ind_aq)                                    as ind_aq,
       coalesce(ao.quote_override, ca.quotes_aq)                               as quotes_aq,
       get_best_consumption(coalesce(ao.igl_ind_override, ca.igl_ind_aq),
                            coalesce(ao.ind_override, ca.ind_aq),
                            coalesce(ao.ann_cons_override, ca.pa_cons_gas),
                            coalesce(ao.quote_override, ca.quotes_aq), 'gas') as ca_source,
       case ca_source
           when 'pa_cons_gas' then coalesce(ao.ann_cons_override, ca.pa_cons_gas)
           when 'igl_ind_aq' then coalesce(ao.igl_ind_override, ca.igl_ind_aq)
           when 'ind_aq' then coalesce(ao.ind_override, ca.ind_aq)
           when 'quotes_aq' then coalesce(ao.quote_override, ca.quotes_aq)
           end                                                                  as ca_value,
       ca.etlchangetype != 'r'                                                  as live,
       ca.etlchange
from (select *
      from (select *, row_number() over (partition by account_id order by etlchange desc) as recency
            from ref_consumption_accuracy_gas_audit) ordered_updates
      where ordered_updates.recency = 1
     ) ca
         left join vw_cons_acc_account_overrides ao
                   on ca.account_id = ao.account_id and ao.meterpointtype = 'G'
order by account_id;