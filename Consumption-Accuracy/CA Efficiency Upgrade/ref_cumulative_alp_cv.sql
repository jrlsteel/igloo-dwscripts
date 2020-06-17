-- truncate table ref_cumulative_alp_cv;
-- insert into ref_cumulative_alp_cv
select cwaalps.coeff_date,
       cwaalps.ldz,
       cwaalp,
       waalp_count,
       (select sum(raic.value / 2)
        from ref_alp_igloo_cv raic
        where raic.ldz = cwaalps.ldz
          and raic.applicable_for < cwaalps.coeff_date) as ccv,
       (select count(raic.value)
        from ref_alp_igloo_cv raic
        where raic.ldz = cwaalps.ldz
          and raic.applicable_for < cwaalps.coeff_date) as cv_count
from (select date                                                                                         as coeff_date,
             ldz,
             (1 + (coalesce(alp.value * 0.5, 0) * (alp.variance))) *
             (alp.forecastdocumentation)                                                                  as waalp,
             sum(waalp)
             over (partition by ldz order by coeff_date rows between unbounded preceding and 1 preceding) as cwaalp,
             count(waalp)
             over (partition by ldz order by coeff_date rows between unbounded preceding and 1 preceding) as waalp_count
      from ref_alp_igloo_daf_wcf alp) cwaalps
order by coeff_date, ldz