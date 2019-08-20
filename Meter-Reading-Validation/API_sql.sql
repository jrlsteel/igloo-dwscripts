select *
from (select account_id,
             register_id,
             last_reading_date,
             last_reading_value,
             igl_eac,
             ind_eac,
             effective_for,
             igl_estimated_advance,
             ind_estimated_advance,
             igl_lower_threshold,
             igl_higher_threshold,
             ind_lower_threshold,
             null as ind_inner_threshold,
             ind_higher_threshold,
             register_num_digits,
             'E'  as type,
             etlchange
      from ref_estimated_advance_elec
      union
      select account_id,
             register_id,
             last_reading_date,
             last_reading_value,
             igl_aq,
             ind_aq,
             effective_for,
             igl_estimated_advance,
             ind_estimated_advance,
             igl_lower_threshold,
             igl_higher_threshold,
             ind_lower_threshold,
             ind_inner_threshold,
             ind_higher_threshold,
             register_num_digits,
             'G' as type,
             etlchange
      from ref_estimated_advance_gas) ref_estimated_advances
where account_id = 1831 and register_id in (14649)