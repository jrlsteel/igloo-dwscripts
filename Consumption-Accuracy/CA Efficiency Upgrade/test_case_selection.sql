truncate table temp_readings_today;
INSERT INTO temp_readings_today (account_id, meterreadingdatetime, meter_id)
VALUES (14018, '2020-06-17 07:00:10.356323000', '22979'),
       (101464, '2020-06-17 07:00:25.538729000', '162208'),
       (54518, '2020-06-17 07:00:34.593063000', '86040'),
-- More rows omitted
       (80754, '2020-06-17 17:21:49.726344000', '129508');



truncate table temp_test_account_selection;
insert into temp_test_account_selection
with dcf_distinct as (select *
                      from (select *, row_number() over (partition by account_id) as rn
                            from ref_calculated_daily_customer_file) ordered
                      where rn = 1)
select dcf.account_id,
       sum((mp.meterpointtype = 'E')::int)                                         as num_elec,
       sum((mp.meterpointtype = 'E' and reading_counts.num_reads = 0)::int)        as zero_read_elec_mets,
       sum((mp.meterpointtype = 'E' and reading_counts.num_reads = 1)::int)        as one_read_elec_mets,
       sum((mp.meterpointtype = 'E' and reading_counts.num_reads > 1)::int)        as many_read_elec_mets,
       sum((mp.meterpointtype = 'G')::int)                                         as num_gas,
       sum((mp.meterpointtype = 'G' and reading_counts.num_reads = 0)::int)        as zero_read_gas_mets,
       sum((mp.meterpointtype = 'G' and reading_counts.num_reads = 1)::int)        as one_read_gas_mets,
       sum((mp.meterpointtype = 'G' and reading_counts.num_reads > 1)::int)        as many_read_gas_mets,
       max(case when mp.meterpointtype = 'E' then met.installeddate else null end) as latest_elec_met_install,
       max(case when mp.meterpointtype = 'G' then met.installeddate else null end) as latest_gas_met_install,
       sum((mp.meterpointtype = 'E' and reading_counts.num_reads > 1 and
            rt.meter_id is not null)::int)                                         as read_elec_today,
       sum((mp.meterpointtype = 'E' and reading_counts.num_reads = 0 and
            rt.meter_id is not null)::int)                                         as first_read_elec_today,
       sum((mp.meterpointtype = 'E' and reading_counts.num_reads = 1 and
            rt.meter_id is not null)::int)                                         as second_read_elec_today,
       sum((mp.meterpointtype = 'G' and reading_counts.num_reads > 1 and
            rt.meter_id is not null)::int)                                         as read_gas_today,
       sum((mp.meterpointtype = 'G' and reading_counts.num_reads = 0 and
            rt.meter_id is not null)::int)                                         as first_read_gas_today,
       sum((mp.meterpointtype = 'G' and reading_counts.num_reads = 1 and
            rt.meter_id is not null)::int)                                         as second_read_gas_today,
       min(dcf.acc_ssd)                                                            as account_start

from dcf_distinct dcf
         left join ref_meterpoints mp on dcf.account_id = mp.account_id and
                                         nvl(least(mp.associationenddate, mp.supplyenddate), getdate() + 1) > getdate()
         left join ref_meters met
                   on dcf.account_id = met.account_id and mp.meter_point_id = met.meter_point_id and removeddate is null
         left join (select account_id, meter_id, count(*) as num_reads
                    from ref_readings_internal_valid
                    group by account_id, meter_id) reading_counts
                   on met.account_id = reading_counts.account_id and met.meter_id = reading_counts.meter_id
         left join (select distinct account_id, meter_id from temp_readings_today) rt
                   on rt.account_id = dcf.account_id and rt.meter_id = mp.meter_point_id
where account_status = 'Live'
group by dcf.account_id
order by account_id

