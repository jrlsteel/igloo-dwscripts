-- remake igloo tariffs
drop table if exists temp_igloo_tariffs;
create table temp_igloo_tariffs as
with most_recent_tariffs as (
    select *
    from (
             select ta.account_id,
                    ta.start_date                                                                      as ta_start,
                    ta.end_date                                                                        as ta_end,
                    t.id                                                                               as tariff_id,
                    t.fuel_type,
                    t.gsp_ldz                                                                          as tariff_gsp,
                    t.unit_rate,
                    t.standing_charge,
                    row_number() over (partition by account_id, fuel_type order by ta.start_date desc) as rn
             from ref_calculated_tariff_accounts ta
                      left join ref_tariffs t on ta.tariff_id = t.id
         ) ordered_tariff_accounts
    where rn = 1
)
select dcf.account_id,
       mrt_elec.standing_charge as elec_sc,
       mrt_elec.unit_rate       as elec_ur,
       mrt_gas.standing_charge  as gas_sc,
       mrt_gas.unit_rate        as gas_ur,
       getdate()                as etlchange
from ref_calculated_daily_customer_file dcf
         left join most_recent_tariffs mrt_elec
                   on dcf.account_id = mrt_elec.account_id and mrt_elec.fuel_type = 'E'
         left join most_recent_tariffs mrt_gas on dcf.account_id = mrt_gas.account_id and mrt_gas.fuel_type = 'G'
-- remake ensek tariffs
drop table if exists temp_ensek_tariffs;
create table temp_ensek_tariffs as
select dcf.account_id,
       elec_sc,
       elec_ur,
       gas_sc,
       gas_ur,
       getdate() as etlchange
from ref_calculated_daily_customer_file dcf
         left join vw_latest_rates lr on dcf.account_id = lr.account_id;
-- remake audit table for comparison results
drop table if exists temp_tariffs_audit;
create table temp_tariffs_audit
(
    account_id    bigint,
    elec_sc       double precision,
    elec_ur       double precision,
    gas_sc        double precision,
    gas_ur        double precision,
    etlchangetype varchar(1),
    etlchange     timestamp
);
-- remake diffs table for comparison results
drop table if exists temp_tariffs_diffs;
create table temp_tariffs_diffs
(
    account_id    bigint,
    field         varchar(100),
    old_val       varchar(65535),
    new_val       varchar(65535),
    old_etlchange timestamp,
    new_etlchange timestamp
);

-- set up compare_sql config table
truncate table ref_compare_sql_config;
INSERT INTO ref_compare_sql_config (old_table, new_table, key_cols, audit_table, diffs_table)
VALUES ('temp_ensek_tariffs', 'temp_igloo_tariffs', 'account_id', 'temp_tariffs_audit', 'temp_tariffs_diffs');

-- RUN GLUE JOB compare_sql NOW

-- ###### analysis of results ######
-- wait for compare_sql glue job to complete

select count(*)
from temp_tariffs_diffs; -- 7134
select count(*)
from temp_tariffs_audit;
-- 3154

-- elec null value counts
select field, elec_reg_status, old_val as ensek_null, new_val as igloo_null, count(*)
from (select account_id, field, old_val is null as old_val, new_val is null as new_val from temp_tariffs_diffs) ttd
         left join ref_calculated_daily_customer_file dcf on dcf.account_id = ttd.account_id
where /*dcf.elec_reg_status in ('Live', 'Pending Final') and*/ left(ttd.field, 1) = 'e'
group by field, elec_reg_status, old_val, new_val
order by field, elec_reg_status, old_val, new_val;

select ttd.account_id,
       ttd.field,
       ttd.old_val         as ensek,
       ttd.new_val         as igloo,
       dcf.gsp,
       trunc(dcf.elec_ssd) as elec_start,
       trunc(dcf.elec_ed)  as elec_end,
       trunc(dcf.gas_ssd)  as gas_start,
       trunc(dcf.gas_ed)   as gas_end
from temp_tariffs_diffs ttd
         left join ref_calculated_daily_customer_file dcf on ttd.account_id = dcf.account_id
where ((left(field, 1) = 'e' and dcf.elec_reg_status in ('Live', 'Pending Final', 'Final'))
    or (left(field, 1) = 'g' and dcf.gas_reg_status in ('Live', 'Pending Final', 'Final')))
  and (igloo is null or ensek is null)
order by account_id


select *
from vw_latest_rates
where account_id = 56769
select *
from ref_tariff_history_elec_ur
where account_id = 1831
select max(account_id)
from ref_tariff_history_gas_ur_audit

select *
from ref_calculated_daily_customer_file
where account_id = 122846

/*
FINDINGS

accounts which close in the period between signup start date and billing start date of a tariff seem to be moved onto
the new tariff early by ensek (they end on the new one which shouldn't have been live for them)

Accounts where elec closes but gas remains still have elec tariff updated by ensek so it doesn't remain as their most
recent elec tariff to be active



*/


create view vw_igloo_ensek_tariff_diffs as
select nvl(igl_trf.account_id, ens_trf.account_id, dcf.account_id) as account_id,
       dcf.account_status,
       dcf.elec_reg_status,
       dcf.gas_reg_status,
       igl_trf.elec_live                                           as igl_elec_live,
       ens_trf.elec_live                                           as ens_elec_live,
       igl_trf.elec_sc                                             as igl_elec_sc,
       ens_trf.elec_sc                                             as ens_elec_sc,
       igl_trf.elec_ur                                             as igl_elec_ur,
       ens_trf.elec_ur                                             as ens_elec_ur,
       igl_trf.gas_live                                            as igl_gas_live,
       ens_trf.gas_live                                            as ens_gas_live,
       igl_trf.gas_sc                                              as igl_gas_sc,
       ens_trf.gas_sc                                              as ens_gas_sc,
       igl_trf.gas_ur                                              as igl_gas_ur,
       ens_trf.gas_ur                                              as ens_gas_ur
from ref_calculated_daily_customer_file dcf
         left join (select account_id,
                           elec_tariff_start is not null and
                           nvl(elec_tariff_end, current_timestamp + 1) >= current_timestamp as elec_live,
                           elec_sc,
                           elec_ur,
                           gas_tariff_start is not null and
                           nvl(gas_tariff_end, current_timestamp + 1) >= current_timestamp  as gas_live,
                           gas_sc,
                           gas_ur
                    from vw_latest_rates_igloo) igl_trf on dcf.account_id = igl_trf.account_id
         left join (select account_id,
                           elec_tariff_start is not null and
                           nvl(elec_tariff_end, current_timestamp + 1) >= current_timestamp as elec_live,
                           elec_sc,
                           elec_ur,
                           gas_tariff_start is not null and
                           nvl(gas_tariff_end, current_timestamp + 1) >= current_timestamp  as gas_live,
                           gas_sc,
                           gas_ur
                    from vw_latest_rates_ensek) ens_trf on dcf.account_id = ens_trf.account_id
where (nvl(dcf.elec_reg_status, '') in ('Live', 'Pending Live', 'Pending Final')
    and (nvl(ens_trf.elec_ur, -1) != nvl(igl_trf.elec_ur, -1)
        or nvl(ens_trf.elec_sc, -1) != nvl(igl_trf.elec_sc, -1)
        or (not ens_trf.elec_live)))
   or (nvl(dcf.gas_reg_status, '') in ('Live', 'Pending Live', 'Pending Final')
    and (nvl(ens_trf.gas_ur, -1) != nvl(igl_trf.gas_ur, -1)
        or nvl(ens_trf.gas_sc, -1) != nvl(igl_trf.gas_sc, -1)
        or (not ens_trf.gas_live)))
order by account_id;

select *
from vw_igloo_ensek_tariff_diffs


select *
from ref_calculated_daily_customer_file
where account_id = 7952

select *
from ref_cdb_supply_contracts
where external_id = 7952

select '2019' is not null and nvl(null, current_timestamp + 1) >= current_timestamp

select *
from ref_calculated_daily_customer_file dcf
         left join ref_tariffs t on dcf.gsp = t.gsp_ldz
where elec_ssd < signup_start_date
  and signup_start_date < gas_ssd
  and gas_ssd < billing_start_date

