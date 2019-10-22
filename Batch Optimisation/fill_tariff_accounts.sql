truncate table ref_tariff_accounts;
insert into ref_tariff_accounts;
drop view vw_tariff_accounts
create or replace view vw_tariff_accounts as
select cf.account_id,
       acc_tariffs.id                                                       as tariff_id,
       trunc(case
                 when wl0_date >= acc_tariffs.signup_start_date then cf.ssd -- starting tariff
                 else greatest(cf.ssd, acc_tariffs.billing_start_date) end) as start_date,
       trunc(least(cf.sed, acc_tariffs.end_date))                           as end_date
from (select account_id,
             wl0_date,
             gsp,
             least(elec_ssd, gas_ssd) as ssd,
             case supply_type --account supply end date
                 when 'Dual' then case
                                      when elec_ed is null or gas_ed is null then null
                                      else greatest(elec_ed, gas_ed) end
                 when 'Gas' then gas_ed
                 when 'Elec' then elec_ed
                 end                  as sed
      from vw_customer_file) cf
         -- starting tariff is joined only if customer signed up in the period of time between signup start date
         -- and billing start date for a tariff
         left join vw_tariffs_combined starting_tariff on cf.wl0_date >= starting_tariff.signup_start_date and
                                                          cf.wl0_date <= starting_tariff.billing_start_date and
                                                          cf.gsp = starting_tariff.gsp_ldz
         left join vw_tariffs_combined acc_tariffs on cf.gsp = acc_tariffs.gsp_ldz and
                                                      ((acc_tariffs.billing_start_date >= cf.wl0_date and
                                                        acc_tariffs.billing_start_date <=
                                                        nvl(cf.sed, '2100-01-01'))
                                                          or
                                                       (starting_tariff.id is null and
                                                        nvl(acc_tariffs.end_date, '2099-12-31') >= cf.wl0_date and
                                                        nvl(acc_tariffs.end_date, '2099-12-31') <=
                                                        nvl(cf.sed, '2100-01-01'))
                                                          )
;

-- ((cf.supply_type in ('Elec', 'Dual') and acc_tariffs.fuel_type = 'E' and
--     ((acc_tariffs.billing_start_date >= cf.wl0_date and
--     acc_tariffs.billing_start_date <= nvl(cf.elec_ed, '2100-01-01'))
--     or
--     (starting_tariff.id is null and
--     nvl(acc_tariffs.end_date, '2099-12-31') >= cf.wl0_date and
--     nvl(acc_tariffs.end_date, '2099-12-31') <=
--     nvl(cf.elec_ed, '2100-01-01'))
--     ))
--     or (cf.supply_type in ('Gas', 'Dual') and acc_tariffs.fuel_type = 'G' and
--     ((acc_tariffs.billing_start_date >= cf.wl0_date and
--     acc_tariffs.billing_start_date <= nvl(cf.gas_ed, '2100-01-01'))
--     or
--     (starting_tariff.id is null and
--     nvl(acc_tariffs.end_date, '2099-12-31') >= cf.wl0_date and
--     nvl(acc_tariffs.end_date, '2099-12-31') <=
--     nvl(cf.gas_ed, '2100-01-01'))
--     )))

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
-- truncate table ref_tariff_history_generated
-- insert into ref_tariff_history_generated
drop view vw_tariff_history
create or replace view vw_tariff_history as
select distinct ta.account_id,
                t.name        as tariff_name,
                ta.start_date as start_date,
                ta.end_date   as end_date,
                '[]'          as discounts,
                'Variable'    as tariff_type,
                null          as exit_fees
from vw_tariff_accounts ta
         left join vw_tariffs_combined t on ta.tariff_id = t.id
order by account_id, start_date

-- auto-generated definition
-- create table ref_tariff_history_generated
-- (
--     account_id  bigint encode delta distkey,
--     tariff_name varchar(255),
--     start_date  timestamp,
--     end_date    timestamp,
--     discounts   varchar(255),
--     tariff_type varchar(255),
--     exit_fees   varchar(255)
-- )
--     diststyle key;
--
-- alter table ref_tariff_history_generated
--     owner to igloo;

create view vw_compare_tariff_histories as
select coalesce(v1.account_id, v2.account_id) as account_id,
       case
           when v1.account_id is not null and v2.account_id is null then 'V1'
           when v1.account_id is null and v2.account_id is not null then 'V2'
           else null end                      as agreement,
       trunc(v1.start_date) as start_date_1,
       v2.start_date as start_date_2,
       trunc(v1.end_date) as end_date_1,
       v2.end_date as end_date_2
from ref_tariff_history v1
         full join vw_tariff_history v2 on v1.account_id = v2.account_id and v1.start_date = v2.start_date
-- where v1.account_id is null or v2.account_id is null
-- where coalesce(v1.account_id, v2.account_id) = 45688
where agreement is not null
order by coalesce(v1.account_id, v2.account_id), coalesce(v1.start_date, v2.start_date)