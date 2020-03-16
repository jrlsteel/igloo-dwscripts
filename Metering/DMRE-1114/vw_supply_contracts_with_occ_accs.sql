create view vw_supply_contracts_with_occ_accs as
SELECT ref_cdb_supply_contracts.id,
         ref_cdb_supply_contracts.supply_address_id,
         ref_cdb_supply_contracts.registration_id,
         ref_cdb_supply_contracts.external_id,
         ref_cdb_supply_contracts.external_uuid,
         ref_cdb_supply_contracts.status,
         ref_cdb_supply_contracts.created_at,
         ref_cdb_supply_contracts.updated_at,
         'sc' AS source
  FROM ref_cdb_supply_contracts
  UNION SELECT -1                                      AS id,
               "max"(mp_address_ids.supply_address_id) AS supply_address_id,
               -1                                      AS registration_id,
               mpr.account_id                          AS external_id,
               '-1'                                    AS external_uuid,
               NULL :: "unknown"                       AS status,
               NULL :: "unknown"                       AS created_at,
               NULL :: "unknown"                       AS updated_at,
               'mpr'                                   AS source
        FROM ((ref_meterpoints_raw mpr LEFT JOIN (SELECT mpr.meterpointnumber,
                                                         "max"(sc.supply_address_id) AS supply_address_id
                                                  FROM (ref_meterpoints_raw mpr LEFT JOIN ref_cdb_supply_contracts sc ON ((
                                                    mpr.account_id = sc.external_id)))
                                                  GROUP BY mpr.meterpointnumber) mp_address_ids ON ((
          mp_address_ids.meterpointnumber = mpr.meterpointnumber))) LEFT JOIN ref_cdb_supply_contracts sc ON ((
          mpr.account_id = sc.external_id)))
        WHERE (sc.external_id IS NULL)
        GROUP BY mpr.account_id
;

alter table vw_supply_contracts_with_occ_accs owner to igloo
;

