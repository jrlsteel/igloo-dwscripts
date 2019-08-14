SELECT
	$__time(range.d),
	COUNT(*) as "MPANs"
FROM (
	SELECT
		date as d
	FROM
		ref_date
	WHERE d > '2017-01-01' and d < CURRENT_TIMESTAMP) range
LEFT JOIN ref_meterpoints m ON m.meterpointtype = 'E'
	AND m.supplystartdate < range.d
	AND (m.supplyenddate >= range.d OR m.supplyenddate is null)
GROUP BY range.d
order by range.d asc;

SELECT
	$__time(range.d),
	run_time as runtime
FROM (
	SELECT
		date as d
	FROM
		ref_date
	WHERE d > '2017-01-01' and d < CURRENT_TIMESTAMP) range
left join (SELECT l.job_id,
       l.job_name,
       l.job_script_name,
       l.job_start,
       l.job_end,
       l.job_status,
       LTRIM(DATEADD(seconds, DATEDIFF(second, job_start, nvl(job_end, getdate())), '1900-01-01 00:00:00'),
             '1900-01-01') as run_time
from dwh_job_logs l
where trunc(job_start) >= trunc(getdate()) - 2
--and job_name = 'all_non_pa_jobs'
order by job_start desc
) j on trunc(j.job_start) = range.d
	GROUP BY range.d
order by range.d asc;

SELECT l.job_id,
       l.job_name,
       l.job_script_name,
       l.job_start,
       l.job_end,
       l.job_status,
			DATEDIFF(second, job_start, nvl(job_end, getdate())) total_secs,
       LTRIM(DATEADD(second, DATEDIFF(second, job_start, nvl(job_end, getdate())), '1900-01-01 00:00:00'),
             '1900-01-01') as run_time
from dwh_job_logs l
where trunc(job_start) >= trunc(getdate())
and job_script_name = 'process_ensek_account_status.py'
order by job_start desc

select * from dwh_job_logs where trunc(job_start) = trunc(getdate()) order by job_start

