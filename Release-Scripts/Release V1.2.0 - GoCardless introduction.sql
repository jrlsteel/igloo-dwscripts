create table ref_fin_gocardless_refunds
(
    ensekid    varchar(50),
    amount     bigint,
    created_at timestamp,
    currency   varchar(50),
    id         varchar(50) distkey,
    mandate    varchar(50),
    metadata   varchar(50),
    payment    varchar(50),
    reference  varchar(250),
    status     varchar(50),
    etlchange  timestamp
)
    diststyle key
    sortkey (id);

alter table ref_fin_gocardless_refunds
    owner to igloo;

create table ref_fin_gocardless_refunds_audit
(
    ensekid       varchar(50),
    amount        bigint,
    created_at    timestamp,
    currency      varchar(50),
    id            varchar(50) distkey,
    mandate       varchar(50),
    metadata      varchar(50),
    payment       varchar(50),
    reference     varchar(250),
    status        varchar(50),
    etlchangetype varchar(1),
    etlchange     timestamp
)
    diststyle key
    sortkey (id);

alter table ref_fin_gocardless_refunds_audit
    owner to igloo;

create table ref_fin_gocardless_mandates
(
    mandate_id                varchar(50) distkey,
    customerid                varchar(50),
    new_mandate_id            varchar(50),
    created_at                timestamp,
    next_possible_charge_date timestamp,
    payments_require_approval boolean,
    reference                 varchar(250),
    scheme                    varchar(50),
    status                    varchar(50),
    creditor                  varchar(150),
    customer_bank_account     varchar(150),
    ensekid                   integer,
    ensekstatementid          integer,
    etlchange                 timestamp
)
    diststyle key
    sortkey (mandate_id);

alter table ref_fin_gocardless_mandates
    owner to igloo;

create table ref_fin_gocardless_mandates_audit
(
    mandate_id                varchar(50) distkey,
    customerid                varchar(50),
    new_mandate_id            varchar(50),
    created_at                timestamp,
    next_possible_charge_date timestamp,
    payments_require_approval boolean,
    reference                 varchar(250),
    scheme                    varchar(50),
    status                    varchar(50),
    creditor                  varchar(150),
    customer_bank_account     varchar(150),
    ensekid                   integer,
    ensekstatementid          integer,
    etlchangetype             varchar(1),
    etlchange                 timestamp
)
    diststyle key
    sortkey (mandate_id);

alter table ref_fin_gocardless_mandates_audit
    owner to igloo;

create table ref_fin_gocardless_subscriptions
(
    id                  varchar(50) distkey,
    created_at          timestamp,
    amount              bigint,
    currency            varchar(50),
    status              varchar(50),
    name                varchar(50),
    start_date          timestamp,
    end_date            timestamp,
    interval            varchar(50),
    interval_unit       varchar(50),
    day_of_month        bigint,
    month               varchar(50),
    count_no            varchar(50),
    payment_reference   varchar(250),
    app_fee             integer,
    retry_if_possible   boolean,
    mandate             varchar(50),
    charge_date         timestamp,
    amount_subscription bigint,
    etlchange           timestamp
)
    diststyle key
    sortkey (id);

alter table ref_fin_gocardless_subscriptions
    owner to igloo;

create table ref_fin_gocardless_subscriptions_audit
(
    id                  varchar(50) distkey,
    created_at          timestamp,
    amount              bigint,
    currency            varchar(50),
    status              varchar(50),
    name                varchar(50),
    start_date          timestamp,
    end_date            timestamp,
    interval            varchar(50),
    interval_unit       varchar(50),
    day_of_month        bigint,
    month               varchar(50),
    count_no            varchar(50),
    payment_reference   varchar(250),
    app_fee             integer,
    retry_if_possible   boolean,
    mandate             varchar(50),
    charge_date         timestamp,
    amount_subscription bigint,
    etlchangetype       varchar(1),
    etlchange           timestamp
)
    diststyle key
    sortkey (id);

alter table ref_fin_gocardless_subscriptions_audit
    owner to igloo;

create table ref_fin_gocardless_payments
(
    id              varchar(50) distkey,
    amount          bigint,
    amount_refunded bigint,
    charge_date     timestamp,
    created_at      timestamp,
    currency        varchar(50),
    description     varchar(250),
    reference       varchar(250),
    status          varchar(50),
    payout          varchar(50),
    mandate         varchar(50),
    subscription    varchar(50),
    ensekid         integer,
    statementid     integer,
    etlchange       timestamp
)
    diststyle key
    sortkey (id);

alter table ref_fin_gocardless_payments
    owner to igloo;


create or replace view public.vw_gocardless_customer_id_mapping as
select distinct gc_users.client_id,
                nvl(gc_users.ensekid, idl2.accountid, idl.accountid, sc.external_id) as igl_acc_id
from aws_fin_stage1_extracts.fin_go_cardless_api_clients gc_users
         left join public.ref_cdb_users igl_users
                   on replace(gc_users.email, ' ', '') = replace(igl_users.email, ' ', '')
         left join public.ref_cdb_user_permissions up
                   on up.permissionable_type = 'App\\SupplyContract' and permission_level = 0 and user_id = igl_users.id
         left join public.ref_cdb_supply_contracts sc on up.permissionable_id = sc.id
         left join aws_fin_stage1_extracts.fin_go_cardless_id_lookup idl on idl.customerid = gc_users.client_id
         left join aws_fin_stage1_extracts.fin_go_cardless_id_mandate_lookup idl2
                   on idl2.customerid = gc_users.client_id
where igl_acc_id is not null
    with no schema binding