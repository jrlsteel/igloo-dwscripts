-- 1
create table ref_consumption_accuracy_override_meterpoints
(
    meterpoint_id    integer,
    override_type_id integer,
    effective_from   timestamp,
    effective_to     timestamp,
    notes            varchar(500)
);

alter table ref_consumption_accuracy_override_meterpoints
    owner to igloo;

-- 2
create table ref_consumption_accuracy_override_types
(
    id                integer distkey,
    name              varchar(20),
    description       varchar(500),
    igl_ind_override  integer,
    ind_override      integer,
    ann_cons_override integer,
    quote_override    integer,
    effective_from    timestamp default getdate(),
    effective_to      timestamp,
    jira              varchar(20)
)
    diststyle key
    sortkey (effective_to, effective_from, id);

alter table ref_consumption_accuracy_override_types
    owner to igloo;

-- 3
create or replace view vw_cons_acc_account_overrides as
select rm.account_id,
       rm.meterpointtype,
       max(ot.igl_ind_override)  as igl_ind_override,
       max(ot.ind_override)      as ind_override,
       max(ot.ann_cons_override) as ann_cons_override,
       max(ot.quote_override)    as quote_override
from ref_meterpoints rm
         left join ref_consumption_accuracy_override_meterpoints om
                   on rm.meter_point_id = om.meterpoint_id and om.effective_from <= getdate() and
                      (nvl(om.effective_to, getdate() + 1) > getdate())
         left join ref_consumption_accuracy_override_types ot
                   on om.override_type_id = ot.id and ot.effective_from <= getdate() and
                      (nvl(ot.effective_to, getdate() + 1) > getdate())
group by rm.account_id, rm.meterpointtype

-- 4
create or replace view vw_cons_acc_elec as
select ca.account_id,
       ca.reading_datetime,
       coalesce(ao.ann_cons_override, ca.pa_cons_elec)                         as pa_cons_elec,
       coalesce(ao.igl_ind_override, ca.igl_ind_eac)                           as igl_ind_eac,
       coalesce(ao.ind_override, ca.ind_eac)                                   as ind_eac,
       coalesce(ao.quote_override, ca.quotes_eac)                              as quotes_eac,
       get_best_consumption(coalesce(ao.igl_ind_override, ca.igl_ind_eac),
                            coalesce(ao.ind_override, ca.ind_eac),
                            coalesce(ao.ann_cons_override, ca.pa_cons_elec),
                            coalesce(ao.quote_override, ca.quotes_eac), 'elec') as ca_source,
       case ca_source
           when 'pa_cons_elec' then coalesce(ao.ann_cons_override, ca.pa_cons_elec)
           when 'igl_ind_eac' then coalesce(ao.igl_ind_override, ca.igl_ind_eac)
           when 'ind_eac' then coalesce(ao.ind_override, ca.ind_eac)
           when 'quotes_eac' then coalesce(ao.quote_override, ca.quotes_eac)
           end                                                                as ca_value,
       ca.etlchange
from ref_consumption_accuracy_elec ca
         left join vw_cons_acc_account_overrides ao
                   on ca.account_id = ao.account_id and ao.meterpointtype = 'E';

-- 5
create or replace view vw_cons_acc_gas as
select ca.account_id,
       ca.reading_datetime,
       coalesce(ao.ann_cons_override, ca.pa_cons_gas)                         as pa_cons_gas,
       coalesce(ao.igl_ind_override, ca.igl_ind_aq)                           as igl_ind_aq,
       coalesce(ao.ind_override, ca.ind_aq)                                   as ind_aq,
       coalesce(ao.quote_override, ca.quotes_aq)                              as quotes_aq,
       get_best_consumption(coalesce(ao.igl_ind_override, ca.igl_ind_aq),
                            coalesce(ao.ind_override, ca.ind_aq),
                            coalesce(ao.ann_cons_override, ca.pa_cons_gas),
                            coalesce(ao.quote_override, ca.quotes_aq), 'gas') as ca_source,
       case ca_source
           when 'pa_cons_gas' then coalesce(ao.ann_cons_override, ca.pa_cons_gas)
           when 'igl_ind_aq' then coalesce(ao.igl_ind_override, ca.igl_ind_aq)
           when 'ind_aq' then coalesce(ao.ind_override, ca.ind_aq)
           when 'quotes_aq' then coalesce(ao.quote_override, ca.quotes_aq)
           end                                                                as ca_value,
       ca.etlchange
from ref_consumption_accuracy_gas ca
         left join vw_cons_acc_account_overrides ao
                   on ca.account_id = ao.account_id and ao.meterpointtype = 'G';

-- 6
drop view vw_cons_acc_gas_on_demand;
create or replace view vw_cons_acc_gas_on_demand as
select batch.account_id,
       nullif(demand.latest_gas_read_date, '1970-01-01')                         as reading_datetime,
       coalesce(ao.ann_cons_override, demand.annualised_consumption)             as pa_cons_gas,
       coalesce(ao.igl_ind_override, demand.igl_ind_aq)                          as igl_ind_aq,
       coalesce(ao.ind_override, batch.ind_aq)                                   as ind_aq,
       coalesce(ao.quote_override, batch.quotes_aq)                              as quotes_aq,
       get_best_consumption(coalesce(ao.igl_ind_override, demand.igl_ind_aq),
                            coalesce(ao.ind_override, batch.ind_aq),
                            coalesce(ao.ann_cons_override, demand.annualised_consumption),
                            coalesce(ao.quote_override, batch.quotes_aq), 'gas') as ca_source,
       case ca_source
           when 'pa_cons_gas' then coalesce(ao.ann_cons_override, demand.annualised_consumption)
           when 'igl_ind_aq' then coalesce(ao.igl_ind_override, demand.igl_ind_aq)
           when 'ind_aq' then coalesce(ao.ind_override, batch.ind_aq)
           when 'quotes_aq' then coalesce(ao.quote_override, batch.quotes_aq)
           end                                                                   as ca_value,
       demand.ac_read_days_diff_gas,
       batch.etlchange
from (select acc_id                                       as account_id,
             sum(annualised_consumption)                  as annualised_consumption,
             min(ac_read_days_diff_gas)                   as ac_read_days_diff_gas,
             min(nvl(latest_gas_read_date, '1970-01-01')) as latest_gas_read_date,
             sum(igl_ind_aq)                              as igl_ind_aq
      from (select coalesce(ac.account_id, iie.account_id) as acc_id,
                   coalesce(ac.annualised_consumption, 0)  as annualised_consumption,
                   coalesce(ac.read_days_diff_gas, 0)      as ac_read_days_diff_gas,
                   coalesce(ac.read_max_created_date_gas,
                            iie.read_max_datetime_gas)     as latest_gas_read_date,
                   coalesce(iie.igl_ind_aq, 0)             as igl_ind_aq
            from vw_annualised_consumption_gas_on_demand ac
                     full join vw_igloo_ind_aq_on_demand iie
                               on ac.account_id = iie.account_id and ac.register_id = iie.register_id) register_level
      group by acc_id) demand
         inner join ref_consumption_accuracy_gas batch on batch.account_id = demand.account_id
         left join vw_cons_acc_account_overrides ao on ao.account_id = demand.account_id and ao.meterpointtype = 'G'

-- 7
drop view vw_cons_acc_elec_on_demand;
create or replace view vw_cons_acc_elec_on_demand as
select batch.account_id,
       nullif(demand.latest_elec_read_date, '1970-01-01')                          as reading_datetime,
       coalesce(ao.ann_cons_override, demand.annualised_consumption)               as pa_cons_elec,
       coalesce(ao.igl_ind_override, demand.igl_ind_eac)                           as igl_ind_eac,
       coalesce(ao.ind_override, batch.ind_eac)                                    as ind_eac,
       coalesce(ao.quote_override, batch.quotes_eac)                               as quotes_eac,
       get_best_consumption(coalesce(ao.igl_ind_override, demand.igl_ind_eac),
                            coalesce(ao.ind_override, batch.ind_eac),
                            coalesce(ao.ann_cons_override, demand.annualised_consumption),
                            coalesce(ao.quote_override, batch.quotes_eac), 'elec') as ca_source,
       case ca_source
           when 'pa_cons_elec' then coalesce(ao.ann_cons_override, demand.annualised_consumption)
           when 'igl_ind_eac' then coalesce(ao.igl_ind_override, demand.igl_ind_eac)
           when 'ind_eac' then coalesce(ao.ind_override, batch.ind_eac)
           when 'quotes_eac' then coalesce(ao.quote_override, batch.quotes_eac)
           end                                                                     as ca_value,
       demand.ac_read_days_diff_elec,
       batch.etlchange
from (select acc_id                                        as account_id,
             sum(annualised_consumption)                   as annualised_consumption,
             min(ac_read_days_diff_elec)                   as ac_read_days_diff_elec,
             min(nvl(latest_elec_read_date, '1970-01-01')) as latest_elec_read_date,
             sum(igl_ind_eac)                              as igl_ind_eac
      from (select coalesce(ac.account_id, iie.account_id) as acc_id,
                   coalesce(ac.annualised_consumption, 0)  as annualised_consumption,
                   coalesce(ac.read_days_diff_elec, 0)     as ac_read_days_diff_elec,
                   coalesce(ac.read_max_created_date_elec,
                            iie.read_max_datetime_elec)    as latest_elec_read_date,
                   coalesce(iie.igl_ind_eac, 0)            as igl_ind_eac
            from vw_annualised_consumption_elec_on_demand ac
                     full join vw_igloo_ind_eac_on_demand iie
                               on ac.account_id = iie.account_id and ac.register_id = iie.register_id) register_level
      group by acc_id) demand
         inner join ref_consumption_accuracy_elec batch on batch.account_id = demand.account_id
         left join vw_cons_acc_account_overrides ao on ao.account_id = demand.account_id and ao.meterpointtype = 'E'
;