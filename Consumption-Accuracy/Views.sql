
-- create or replace view vw_consumption_register_accuracy_aq as
SELECT su.external_id          AS account_id,
         su.supply_address_id,
         su.registration_id,
         ac.status,
         mp_gas.meter_point_id   AS meter_point_id_gas,
         mt_gas.meter_id         AS meter_id_gas,
         reg_gas.register_id     AS register_id_gas,
         aq_v1.igloo_aq_v1       AS igl_ind_aq,
         aq_pa.igloo_aq          AS pa_cons_gas,
         reg_gas.registers_eacaq AS ind_aq
  FROM ((((((ref_cdb_supply_contracts su JOIN ref_account_status ac ON ((ac.account_id =
                                                                         su.external_id)))
    JOIN ref_meterpoints mp_gas ON ((
    (mp_gas.account_id = su.external_id) AND
    ((mp_gas.meterpointtype) :: text = 'G' :: text)))) LEFT JOIN ref_meters mt_gas ON ((
    ((mp_gas.account_id = mt_gas.account_id) AND (mp_gas.meter_point_id = mt_gas.meter_point_id)) AND
    (mt_gas.removeddate IS NULL)))) LEFT JOIN ref_registers reg_gas ON (((mt_gas.account_id = reg_gas.account_id) AND
                                                                         (mt_gas.meter_id =
                                                                          reg_gas.meter_id)))) LEFT JOIN ref_calculated_aq aq_pa ON ((
    (aq_pa.account_id = su.external_id) AND
    (aq_pa.register_id = reg_gas.register_id)))) LEFT JOIN ref_calculated_aq_v1 aq_v1 ON ((
    (aq_v1.account_id = su.external_id) AND (aq_v1.register_id = reg_gas.register_id))))
  WHERE ((ac.status) :: text = 'Live' :: text)
  ORDER BY su.external_id
;

alter table vw_consumption_register_accuracy_aq owner to igloo
;

/*View on tolerance*/
create view vw_consumption_account_accuracy_tolerances as
SELECT vw_consumption_account_accuracy.account_id,
         vw_consumption_account_accuracy.igl_ind_eac,
         (CASE
            WHEN (((vw_consumption_account_accuracy.igl_ind_eac IS NULL) OR
                   (vw_consumption_account_accuracy.pa_cons_elec IS NULL)) OR
                  ((vw_consumption_account_accuracy.igl_ind_eac = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.pa_cons_elec = (0) :: double precision)))
                    THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.igl_ind_eac / vw_consumption_account_accuracy.pa_cons_elec) END -
          (1) :: double precision)                                                                                 AS igl_ind_eac_tolerance_cons,
         (CASE
            WHEN (((vw_consumption_account_accuracy.igl_ind_eac IS NULL) OR
                   (vw_consumption_account_accuracy.ind_eac IS NULL)) OR
                  ((vw_consumption_account_accuracy.igl_ind_eac = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.ind_eac = (0) :: double precision))) THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.igl_ind_eac / vw_consumption_account_accuracy.ind_eac) END -
          (1) :: double precision)                                                                                 AS igl_ind_eac_tolerance_ind,
         vw_consumption_account_accuracy.igl_ind_aq,
         (CASE
            WHEN (((vw_consumption_account_accuracy.igl_ind_aq IS NULL) OR
                   (vw_consumption_account_accuracy.pa_cons_gas IS NULL)) OR
                  ((vw_consumption_account_accuracy.igl_ind_aq = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.pa_cons_gas = (0) :: double precision)))
                    THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.igl_ind_aq / vw_consumption_account_accuracy.pa_cons_gas) END -
          (1) :: double precision)                                                                                 AS igl_ind_aq_tolerance_cons,
         (CASE
            WHEN (((vw_consumption_account_accuracy.igl_ind_aq IS NULL) OR
                   (vw_consumption_account_accuracy.ind_aq IS NULL)) OR
                  ((vw_consumption_account_accuracy.igl_ind_aq = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.ind_aq = (0) :: double precision))) THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.igl_ind_aq / vw_consumption_account_accuracy.ind_aq) END -
          (1) :: double precision)                                                                                 AS igl_ind_aq_tolerance_ind,
         vw_consumption_account_accuracy.pa_cons_elec,
         (CASE
            WHEN (((vw_consumption_account_accuracy.pa_cons_elec IS NULL) OR
                   (vw_consumption_account_accuracy.ind_eac IS NULL)) OR
                  ((vw_consumption_account_accuracy.pa_cons_elec = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.ind_eac = (0) :: double precision))) THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.pa_cons_elec / vw_consumption_account_accuracy.ind_eac) END -
          (1) :: double precision)                                                                                 AS pa_cons_elec_tolerance_ind,
         (CASE
            WHEN (((vw_consumption_account_accuracy.pa_cons_elec IS NULL) OR
                   (vw_consumption_account_accuracy.quotes_eac IS NULL)) OR
                  ((vw_consumption_account_accuracy.pa_cons_elec = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.quotes_eac = 0))) THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.pa_cons_elec /
                  (vw_consumption_account_accuracy.quotes_eac) :: double precision) END -
          (1) :: double precision)                                                                                 AS pa_cons_elec_tolerance_quotes,
         vw_consumption_account_accuracy.pa_cons_gas,
         (CASE
            WHEN (((vw_consumption_account_accuracy.pa_cons_gas IS NULL) OR
                   (vw_consumption_account_accuracy.ind_aq IS NULL)) OR
                  ((vw_consumption_account_accuracy.pa_cons_gas = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.ind_aq = (0) :: double precision))) THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.pa_cons_gas / vw_consumption_account_accuracy.ind_aq) END -
          (1) :: double precision)                                                                                 AS pa_cons_gas_tolerance_ind,
         (CASE
            WHEN (((vw_consumption_account_accuracy.pa_cons_gas IS NULL) OR
                   (vw_consumption_account_accuracy.quotes_aq IS NULL)) OR
                  ((vw_consumption_account_accuracy.pa_cons_gas = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.quotes_aq = 0))) THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.pa_cons_gas /
                  (vw_consumption_account_accuracy.quotes_aq) :: double precision) END -
          (1) :: double precision)                                                                                 AS pa_cons_gas_tolerance_quotes,
         vw_consumption_account_accuracy.ind_eac,
         (CASE
            WHEN (((vw_consumption_account_accuracy.ind_eac IS NULL) OR
                   (vw_consumption_account_accuracy.quotes_eac IS NULL)) OR
                  ((vw_consumption_account_accuracy.ind_eac = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.quotes_eac = 0))) THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.ind_eac /
                  (vw_consumption_account_accuracy.quotes_eac) :: double precision) END -
          (1) :: double precision)                                                                                 AS ind_eac_tolerance_quotes,
         vw_consumption_account_accuracy.ind_aq,
         (CASE
            WHEN (((vw_consumption_account_accuracy.ind_aq IS NULL) OR
                   (vw_consumption_account_accuracy.quotes_aq IS NULL)) OR
                  ((vw_consumption_account_accuracy.ind_aq = (0) :: double precision) OR
                   (vw_consumption_account_accuracy.quotes_aq = 0))) THEN (-98) :: double precision
            ELSE (vw_consumption_account_accuracy.ind_aq /
                  (vw_consumption_account_accuracy.quotes_aq) :: double precision) END -
          (1) :: double precision)                                                                                 AS ind_aq_tolerance_quotes,
         vw_consumption_account_accuracy.quotes_eac,
         vw_consumption_account_accuracy.quotes_aq
  FROM vw_consumption_account_accuracy
;

alter table vw_consumption_account_accuracy_tolerances owner to igloo
;

/*View on account*/
create view vw_consumption_account_accuracy as
SELECT eac.account_id,
         sum(eac.igl_ind_eac)  AS igl_ind_eac,
         sum(aq.igl_ind_aq)    AS igl_ind_aq,
         sum(eac.pa_cons_elec) AS pa_cons_elec,
         sum(aq.pa_cons_gas)   AS pa_cons_gas,
         sum(eac.ind_eac)      AS ind_eac,
         sum(aq.ind_aq)        AS ind_aq,
         q_eacaq.quotes_eac,
         q_eacaq.quotes_aq
  FROM ((vw_consumption_register_accuracy_eac eac LEFT JOIN vw_consumption_register_accuracy_aq aq ON ((eac.account_id =
                                                                                                        aq.account_id))) LEFT JOIN
        (SELECT reg.id,
                                           q.user_id,
                                           q.gas_usage         AS quotes_aq,
                                           q.electricity_usage AS quotes_eac
                                    FROM (ref_cdb_registrations reg JOIN ref_cdb_quotes q ON ((
                                      q.id
                                      =
                                      reg.quote_id)))) q_eacaq ON ((
    q_eacaq.id = eac.registration_id)))
  GROUP BY eac.account_id, q_eacaq.quotes_eac, q_eacaq.quotes_aq
  ORDER BY eac.account_id
;

alter table vw_consumption_account_accuracy owner to igloo
;
