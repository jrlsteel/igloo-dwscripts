create table ref_consumption_accuracy_override_meterpoints
(
    meterpoint_id    integer,
    override_type_id integer,
    effective_from   timestamp,
    effective_to     timestamp,
    notes            varchar(500)
);

alter table ref_consumption_accuracy_override_meterpoints
    owner to igloo;

create table ref_consumption_accuracy_override_types
(
    id                integer distkey,
    name              varchar(20),
    description       varchar(500),
    igl_ind_override  integer,
    ind_override      integer,
    ann_cons_override integer,
    quote_override    integer,
    effective_from    timestamp default getdate(),
    effective_to      timestamp,
    jira              varchar(20)
)
    diststyle key
    sortkey (effective_to, effective_from, id);

alter table ref_consumption_accuracy_override_types
    owner to igloo;