create function suitability_rank(bigint, integer) returns decimal
--                             (days_diff, method#)
    stable
    language sql
as
$$

-- it is assumed no open reads will be offered outside of the range of 9 months to 3 years so they are not checked for
select case $2 --method number
    when 1 then
        -- closest read to 1 year prior to closing read. Can be either direction from 1 year but prefers the more
        -- recent value if there are two equally near, one either side.
        abs(364.75-$1)
    when 2 then
        -- first read reached walking from 1 year prior forward in time to 9 months prior, then 366 days back in time
        -- to 3 years
        case when $1 <= 365 then 365-$1 else $1 end
    when 3 then
        -- first read reached walking from 1 year prior forward in time to 9 months prior, then 3 years walking forward
        -- to 366 days
        case when $1 <= 365 then 365-$1 else (365*3)-$1 end
end
$$;
