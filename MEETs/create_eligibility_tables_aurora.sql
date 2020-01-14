drop table ref_meets_eligibility;
create table ref_meets_eligibility
(
    account_id          bigint,
    num_s2_elec         int,
    num_s2_gas          int,
    hh_consent          boolean,
    account_status      varchar(20)
);