
SELECT l.job_id,
               l.job_name,
               l.job_script_name,
               l.job_start,
               l.job_end,
               l.job_status,
                LTRIM(DATEADD(seconds, DATEDIFF(second, job_start, nvl(job_end, getdate())), '1900-01-01 00:00:00'),
                            '1900-01-01') as run_time
            from dwh_job_logs l
where trunc(job_start) = '2019-06-12'
            order by job_start desc;
