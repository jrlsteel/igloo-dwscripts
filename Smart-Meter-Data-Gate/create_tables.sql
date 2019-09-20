create table ref_sme_zones
(
    zone_id    int distkey,
    area_name  varchar(30),
    zone_scope int,
    open_date  timestamp,
    etlchange  timestamp
)
    diststyle key
    sortkey (zone_id);

alter table ref_sme_zones
    owner to igloo;

create table ref_sme_postcode_mapping
(
    zone_id         int distkey,
    postcode_prefix varchar(4)
)
    diststyle key
    sortkey (zone_id, postcode_prefix);

alter table ref_sme_postcode_mapping
    owner to igloo;

create table ref_sme_postcode_overrides
(
    postcode_prefix varchar(4) distkey,
    override_start  timestamp,
    override_end    timestamp,
    reason          varchar(50)
)
    diststyle key
    sortkey (postcode_prefix, override_start);

alter table ref_sme_postcode_overrides
    owner to igloo;

create table ref_sme_accounts
(
    account_id         bigint distkey,
    eligibility_status varchar(10),
    detailed_status    varchar(30),
    eligible_from      timestamp,
    etlchange          timestamp
)
    diststyle key
    sortkey (account_id);

alter table ref_sme_accounts
    owner to igloo;