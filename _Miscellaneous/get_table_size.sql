SELECT TRIM(pgdb.datname) AS db,
       TRIM(pgn.nspname)  AS schema,
       TRIM(a.name)       AS table,
       b.mbytes,
       a.rows,
       b.slices,
       b.cols
 FROM (SELECT db_id,
              id,
              name,
              SUM(rows) AS rows
         FROM stv_tbl_perm a
        GROUP BY db_id, id, name
      ) AS a
  JOIN pg_class AS pgc
    ON pgc.oid = a.id
  JOIN pg_namespace AS pgn
    ON pgn.oid = pgc.relnamespace
  JOIN pg_database AS pgdb
    ON pgdb.oid = a.db_id
  JOIN (SELECT tbl,
               COUNT(*) AS mbytes,
               COUNT(distinct slice) slices,
               COUNT(distinct col) cols
          FROM stv_blocklist
         GROUP BY tbl
       ) b
    ON a.id = b.tbl
 WHERE
       pgn.nspname = 'public' and
       a.name LIKE 'dim%'
 ORDER BY a.db_id,
          a.name