-- liquibase formatted sql

-- changeset BenJesty:1615471763681-1
create view vw_sens_withdrawal_of_consent as
select ext_participant_id,
       consent_end_date    as consentwithdrawaldate,
       address_line1       as addressline1,
       address_line2       as addressline2,
       address_line3       as addressline3,
       address_line4       as addressline4,
       postaltown,
       county,
       postcode,
       firstname           as first_name,
       surname             as last_name,
       'Online'            as source,
       'ConsentWithdrawal' as gdprtype,
       mpan,
       uprn
from public.vw_sens_weekly_master
where withdrawal_type in ('COS', 'OPT_OUT')
with no schema binding;

alter table vw_sens_withdrawal_of_consent
    owner to igloo;

grant select on vw_sens_withdrawal_of_consent to grafana;

grant select on vw_sens_withdrawal_of_consent to igloo_grafana;

grant select on vw_sens_withdrawal_of_consent to public;