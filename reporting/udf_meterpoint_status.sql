create or replace function udf_meterpoint_status(timestamp, timestamp)
--(startdate,enddate)
returns varchar(15)
stable
as $$
-- gets the status of each meterpoint, along with other meterpoint data already listed in the db
select
    case when $1 > $2 then --start after end
        'Cancelled'
    else
        case when current_date < $1 then --current date before start
            'Pending Live'
        else
            case when $2 isnull then --no end date
                'Live'
            else
                case when $2 > current_date then --end date is after current date
                    'Pending Final'
                else
                    'Final'
                end
            end
        end
    end as meterpoint_status

$$ language sql;