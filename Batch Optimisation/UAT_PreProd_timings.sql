select

from (select distinct job_id from dwh_spark_logs) jids
left join (select * from dwh_job_logs where trunc(job_start) = '2019-10-08') job_logs on jids.job_id = job_logs.job_id