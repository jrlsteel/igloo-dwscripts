create function nulls_latest(timestamp,timestamp) returns timestamp
stable language sql as
    $$
        select nullif(greatest(nvl($1,current_date+1000),nvl($2,current_date+1000)),current_date+1000)

        $$