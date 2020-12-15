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
    num_gas_s2           int
)