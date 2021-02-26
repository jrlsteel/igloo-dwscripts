create table ref_meets_eligibility_backup as
select *
from ref_meets_eligibility;

drop table ref_meets_eligibility;
create table ref_meets_eligibility
(
    user_id              bigint,
    ensek_account_id     bigint,
    num_smart_comm_elec  int,
    num_smart_comm_gas   int,
    hh_consent           boolean,
    account_status       varchar(13),
    num_elec             int,
    num_elec_dcc_enabled int,
    num_elec_with_hh     int,
    num_elec_s2          int,
    num_gas              int,
    num_gas_dcc_enabled  int,
    num_gas_with_hh      int,
    num_gas_s2           int,
    etlchange            timestamp
);

insert into ref_meets_eligibility
select null           as user_id,
       account_id     as ensek_account_id,
       num_s2_elec    as num_smart_comm_elec,
       num_s2_gas     as num_smart_comm_gas,
       hh_consent     as hh_consent,
       account_status as account_status,
       null           as num_elec,
       null           as num_elec_dcc_enabled,
       null           as num_elec_with_hh,
       null           as num_elec_s2,
       null           as num_gas,
       null           as num_gas_dcc_enabled,
       null           as num_gas_with_hh,
       null           as num_gas_s2,
       etlchange      as etlchange

from ref_meets_eligibility_backup;

drop table ref_meets_eligibility_backup;