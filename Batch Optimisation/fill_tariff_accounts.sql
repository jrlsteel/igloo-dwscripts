truncate table ref_tariff_accounts;
insert into ref_tariff_accounts
select cf.account_id,
       acc_tariffs.id                                             as tariff_id,
       trunc(case
                 when wl0_date >= acc_tariffs.signup_start_date then
                     case when acc_tariffs.fuel_type = 'E' then elec_ssd else gas_ssd end
                 else
                     case
                         when acc_tariffs.fuel_type = 'E'
                             then greatest(elec_ssd, acc_tariffs.billing_start_date)
                         else greatest(gas_ssd, acc_tariffs.billing_start_date)
                         end
           end)                                                   as start_date,
       trunc(case
                 when acc_tariffs.fuel_type = 'E'
                     then least(cf.elec_ed, acc_tariffs.end_date)
                 else least(cf.gas_ed, acc_tariffs.end_date) end) as end_date
from vw_customer_file cf
         left join ref_tariffs starting_tariff on cf.wl0_date >= starting_tariff.signup_start_date and
                                                  cf.wl0_date <= starting_tariff.billing_start_date and
                                                  cf.gsp = starting_tariff.gsp_ldz and
                                                  starting_tariff.fuel_type = 'E'

         left join ref_tariffs acc_tariffs on cf.gsp = acc_tariffs.gsp_ldz and
                                              ((cf.supply_type in ('Elec', 'Dual') and acc_tariffs.fuel_type = 'E' and
                                                ((acc_tariffs.billing_start_date >= cf.wl0_date and
                                                  acc_tariffs.billing_start_date <= nvl(cf.elec_ed, '2100-01-01'))
                                                    or
                                                 (starting_tariff.id is null and
                                                  nvl(acc_tariffs.end_date, '2099-12-31') >= cf.wl0_date and
                                                  nvl(acc_tariffs.end_date, '2099-12-31') <=
                                                  nvl(cf.elec_ed, '2100-01-01'))
                                                    ))
                                                  or
                                               (cf.supply_type in ('Gas', 'Dual') and acc_tariffs.fuel_type = 'G' and
                                                ((acc_tariffs.billing_start_date >= cf.wl0_date and
                                                  acc_tariffs.billing_start_date <= nvl(cf.gas_ed, '2100-01-01'))
                                                    or
                                                 (starting_tariff.id is null and
                                                  nvl(acc_tariffs.end_date, '2099-12-31') >= cf.wl0_date and
                                                  nvl(acc_tariffs.end_date, '2099-12-31') <=
                                                  nvl(cf.gas_ed, '2100-01-01'))
                                                    )))
;

-- auto-generated definition
create table ref_tariff_accounts
(
    account_id bigint,
    tariff_id  integer,
    start_date timestamp,
    end_date   timestamp
);

alter table ref_tariff_accounts
    owner to igloo;


-- mirroring ref_tariff_history
truncate table ref_tariff_history_generated
insert into ref_tariff_history_generated
select distinct ta.account_id, t.name, trunc(ta.start_date), trunc(ta.end_date), '[]', 'Variable', null
from ref_tariff_accounts ta
         left join ref_tariffs t on ta.tariff_id = t.id
order by account_id, start_date

-- auto-generated definition
create table ref_tariff_history_generated
(
    account_id  bigint encode delta distkey,
    tariff_name varchar(255),
    start_date  timestamp,
    end_date    timestamp,
    discounts   varchar(255),
    tariff_type varchar(255),
    exit_fees   varchar(255)
)
    diststyle key;

alter table ref_tariff_history_generated
    owner to igloo;


select *
from ref_tariff_history v1
         full join ref_tariff_history_generated v2 on v1.account_id = v2.account_id and v1.start_date = v2.start_date
--where v1.account_id is null or v2.account_id is null
where coalesce(v1.account_id, v2.account_id) = 4105

select max(datediff(days, signup_start_date, billing_start_date))
from ref_tariffs