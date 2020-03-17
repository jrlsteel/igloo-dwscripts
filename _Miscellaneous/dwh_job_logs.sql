
SELECT l.job_id,
               l.job_name,
               l.job_script_name,
               to_char(l.job_start,'yyyy-mm-dd hh:mi:ss') job_start,
               to_char(l.job_end,'yyyy-mm-dd hh:mi:ss') job_end,
               l.job_status,
               l.job_error_message,
                LTRIM(DATEADD(seconds, DATEDIFF(second, job_start, nvl(job_end, getdate())), '1900-01-01 00:00:00'),
                            '1900-01-01') as run_time
            from dwh_job_logs l
--where trunc(job_start) = '2019-06-12'
            order by to_char(l.job_start,'yyyy-mm-dd hh:mi:ss')desc;
