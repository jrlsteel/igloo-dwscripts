-- remove certificate_hash from main epc certificates table

drop view vw_epc_property_type_count;

alter table ref_epc_certificates
    drop column certificate_hash;

create view vw_epc_property_type_count(property_type, no_properties) as
SELECT "replace"(derived_table1.property_type::text, 'Enclosed '::character varying::text,
                 ''::character varying::text) AS property_type,
       sum(derived_table1.no_properties)      AS no_properties
FROM (SELECT CASE
                 WHEN ref_epc_certificates.property_type::text = 'House'::character varying::text OR
                      ref_epc_certificates.property_type::text = 'Maisonette'::character varying::text
                     THEN ref_epc_certificates.built_form
                 ELSE ref_epc_certificates.property_type
                 END  AS property_type,
             count(*) AS no_properties
      FROM ref_epc_certificates
      WHERE ref_epc_certificates.property_type::text <> 'NO DATA!'::character varying::text
      GROUP BY CASE
                   WHEN ref_epc_certificates.property_type::text = 'House'::character varying::text OR
                        ref_epc_certificates.property_type::text = 'Maisonette'::character varying::text
                       THEN ref_epc_certificates.built_form
                   ELSE ref_epc_certificates.property_type
                   END) derived_table1
WHERE derived_table1.property_type::text <> 'NO DATA!'::character varying::text
GROUP BY "replace"(derived_table1.property_type::text, 'Enclosed '::character varying::text,
                   ''::character varying::text);
alter table vw_epc_property_type_count
    owner to igloo;


-- change lmk_key data type from bigint to varchar in recommendations table
drop table ref_epc_recommendations;

-- auto-generated definition
create table ref_epc_recommendations
(
    lmk_key                  varchar(50) distkey,
    improvement_item         bigint,
    improvement_summary_text varchar(65535),
    improvement_descr_text   varchar(65535),
    improvement_id           bigint,
    improvement_id_text      varchar(65535)
)
    diststyle key;

alter table ref_epc_recommendations
    owner to igloo;


