create or replace view vw_sens_consent_weekly_report as
select ext_participant_id,
       address_line1,
       address_line2,
       address_line3,
       address_line4,
       postaltown,
       county,
       postcode,
       firstname,
       surname,
       mpan,
       uprn,
       consent_provided,
       consent_start_date,
       consent_end_date,
       consent_source,
       auth_completed,
       auth_date,
       move_in_date,
       project_id,
       trial_id,
       trial_group,
       dcc_enrolled_meter,
       phone_number,
       email
from public.vw_sens_weekly_master
with no schema binding;

alter table vw_sens_consent_weekly_report
    owner to igloo;

grant select on vw_sens_consent_weekly_report to grafana;

grant select on vw_sens_consent_weekly_report to igloo_grafana;

grant select on vw_sens_consent_weekly_report to public;