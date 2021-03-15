create or replace view vw_sens_change_of_tenancy as
select ext_participant_id,
       consent_end_date as dateofhousemove,
       address_line1    as addressline1,
       address_line2    as addressline2,
       address_line3    as addressline3,
       address_line4    as addressline4,
       postaltown,
       county,
       postcode,
       firstname        as first_name,
       surname          as last_name,
       'Telephone'      as source,
       mpan,
       uprn
from public.vw_sens_weekly_master
where withdrawal_type = 'COT'
with no schema binding;

alter table vw_sens_change_of_tenancy
    owner to igloo;

grant select on vw_sens_change_of_tenancy to grafana;

grant select on vw_sens_change_of_tenancy to igloo_grafana;

grant select on vw_sens_change_of_tenancy to public;