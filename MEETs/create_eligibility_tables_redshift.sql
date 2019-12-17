drop table ref_meets_eligibility;
create table ref_meets_eligibility
(
    account_id          bigint distkey,
    num_s2_elec         int,
    num_s2_gas          int,
--     mmh_subset_complete boolean,
    hh_consent          boolean,
    account_status      varchar(20)
)
    diststyle key
    sortkey (account_id);

alter table ref_meets_eligibility
    owner to igloo;