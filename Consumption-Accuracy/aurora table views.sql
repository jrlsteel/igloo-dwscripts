create table ref_consumption_accuracy_elec_vw
(
    account_id       bigint,
    reading_datetime timestamp,
    pa_cons_elec     double precision,
    igl_ind_eac      double precision,
    ind_eac          double precision,
    quotes_eac       double precision,
    ca_source        varchar(256),
    ca_value         double precision,
    etlchange        timestamp
)

create table ref_consumption_accuracy_gas_vw
(
    account_id       bigint,
    reading_datetime timestamp,
    pa_cons_gas      double precision,
    igl_ind_aq       double precision,
    ind_aq           double precision,
    quotes_aq        double precision,
    ca_source        varchar(256),
    ca_value         double precision,
    etlchange        timestamp
)