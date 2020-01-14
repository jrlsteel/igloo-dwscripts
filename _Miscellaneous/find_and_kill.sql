-- find running processes
select pid, user_name, starttime, query
from stv_recents
where status='Running';

-- kill running process with pid = [pid]
cancel [pid];