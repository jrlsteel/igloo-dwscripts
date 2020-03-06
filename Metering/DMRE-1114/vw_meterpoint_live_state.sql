create view vw_meterpoint_live_state as
SELECT mp_stat.account_id,
         udf_meterpoint_status(min(mp_stat.start_date), CASE
                                                          WHEN ("max"(mp_stat.end_date) =
                                                                ((('now' :: text) :: date + 1000)) :: timestamp without time zone)
                                                                  THEN NULL :: timestamp without time zone
                                                          ELSE "max"(mp_stat.end_date) END) AS acc_stat,
         CASE
           WHEN ("max"(mp_stat.aed) = ((('now' :: text) :: date + 1000)) :: timestamp without time zone)
                   THEN NULL :: timestamp without time zone
           ELSE "max"(mp_stat.aed) END                                                      AS aed,
         CASE
           WHEN ("max"(mp_stat.sed) = ((('now' :: text) :: date + 1000)) :: timestamp without time zone)
                   THEN NULL :: timestamp without time zone
           ELSE "max"(mp_stat.sed) END                                                      AS sed,
         CASE WHEN (sum(mp_stat.hmi) = count(mp_stat.hmi)) THEN 1 ELSE 0 END                AS home_move_in
  FROM (SELECT ref_meterpoints.account_id,
               ref_meterpoints.meterpointtype,
               GREATEST(ref_meterpoints.supplystartdate, ref_meterpoints.associationstartdate) AS start_date,
               COALESCE(LEAST(ref_meterpoints.supplyenddate, ref_meterpoints.associationenddate),
                        ((('now' :: text) :: date + 1000)) :: timestamp without time zone)     AS end_date,
               COALESCE(ref_meterpoints.associationenddate,
                        ((('now' :: text) :: date + 1000)) :: timestamp without time zone)     AS aed,
               COALESCE(ref_meterpoints.supplyenddate,
                        ((('now' :: text) :: date + 1000)) :: timestamp without time zone)     AS sed,
               CASE
                 WHEN (ref_meterpoints.associationstartdate > ref_meterpoints.supplystartdate) THEN 1
                 ELSE 0 END                                                                    AS hmi
        FROM ref_meterpoints
        WHERE (((GREATEST(ref_meterpoints.supplystartdate, ref_meterpoints.associationstartdate) <
                 COALESCE(LEAST(ref_meterpoints.supplyenddate, ref_meterpoints.associationenddate),
                          ((('now' :: text) :: date + 1000)) :: timestamp without time zone)) OR
                (COALESCE(LEAST(ref_meterpoints.supplyenddate, ref_meterpoints.associationenddate),
                          ((('now' :: text) :: date + 1000)) :: timestamp without time zone) IS NULL)) AND
               (ref_meterpoints.account_id <> ALL
                (ARRAY[29678::bigint, 36991::bigint, 38044::bigint, 38114::bigint, 38601::bigint, 38602::bigint, 38603::bigint, 38604::bigint, 38605::bigint, 38606::bigint, 38607::bigint, 38741::bigint, 38742::bigint, 41025::bigint, 43866::bigint, 45731::bigint, 46091::bigint, 46605::bigint, 46606::bigint])))) mp_stat
  GROUP BY mp_stat.account_id
  ORDER BY mp_stat.account_id
;

alter table vw_meterpoint_live_state owner to igloo
;

