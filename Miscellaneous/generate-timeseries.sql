select timestamp '2011-12-31 00:00:00' + (i * interval '1 min') as time
from  generate_series(1, (date '2012-12-31' - date '2011-12-31')) i;

-- create view vw_todays_time
--   as

select cast(trunc(getdate()) as timestamp) + (i * interval '1 min') as time
from  generate_series(1, (1440 - 1)) i;

SELECT l.job_id,
       l.job_name,
       l.job_script_name,
       l.job_start,
       l.job_end,
       l.job_status,
       LTRIM(DATEADD(seconds, DATEDIFF(second, job_start, nvl(job_end, getdate())), '1900-01-01 00:00:00'),
             '1900-01-01') as run_time
from dwh_job_logs l
where trunc(job_start) >= trunc(getdate()) - 1
--and job_name = 'all_non_pa_jobs'
order by job_start desc;

SELECT
  ROUND(UNIX_TIMESTAMP(api_performance.start)) as time_sec,
  AVG(api_performance.duration) as value,
  "v1/user/full" as metric
FROM api_performance
WHERE $__timeFilter(api_performance.start) and api_performance.endpoint = 'v1/user/full'
GROUP BY time_sec DIV 120
ORDER BY time_sec ASC

SELECT userid,
       query,
       label,
       xid,
       pid,
       database,
       querytxt,
       starttime,
       endtime,
       aborted,
       insert_pristine,
       concurrency_scaling_status
FROM stl_query
where database in ('igloosense-prod', 'igloosense-uat')
  and querytxt !=
      'select * from stl_query order by starttime desc                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  '
order by starttime desc

