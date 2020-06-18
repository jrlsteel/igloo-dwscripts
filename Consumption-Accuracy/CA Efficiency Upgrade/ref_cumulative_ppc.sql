select trunc(st_date)                                                                    as ppc_date,
       gsp,
       ssc,
       tpr,
       pcl,
       sum(ppc_sum)
       over (partition by gsp, ssc, tpr, pcl
           order by st_date::timestamp rows between unbounded preceding and 1 preceding) as cumulative_ppc,
       count(ppc_sum)
       over (partition by gsp, ssc, tpr, pcl
           order by st_date::timestamp rows between unbounded preceding and 1 preceding) as cumulative_ppc_count
from (select bpp_all.st_date, portfolio_register_types.*, nvl(ppc_sum, bpp_sum) as ppc_sum
      from (select distinct mpa_gsp.attributes_attributevalue      as gsp,
                            mpa_ssc.attributes_attributevalue      as ssc,
                            reg.registers_tpr::bigint              as tpr,
                            mpa_pcl.attributes_attributevalue::int as pcl
            from ref_meterpoints mp
                     inner join ref_meters met on mp.account_id = met.account_id and
                                                  mp.meter_point_id = met.meter_point_id and
                                                  met.removeddate is null
                     inner join ref_registers reg on met.account_id = reg.account_id and
                                                     met.meter_point_id = reg.meter_point_id and
                                                     met.meter_id = reg.meter_id
                     inner join ref_meterpoints_attributes mpa_ssc
                                on mp.account_id = mpa_ssc.account_id and
                                   mp.meter_point_id = mpa_ssc.meter_point_id and
                                   mpa_ssc.attributes_attributename = 'SSC'
                     inner join ref_meterpoints_attributes mpa_gsp
                                on mp.account_id = mpa_gsp.account_id and
                                   mp.meter_point_id = mpa_gsp.meter_point_id and
                                   mpa_gsp.attributes_attributename = 'GSP'
                     inner join ref_meterpoints_attributes mpa_pcl
                                on mp.account_id = mpa_pcl.account_id and
                                   mp.meter_point_id = mpa_pcl.meter_point_id and
                                   mpa_pcl.attributes_attributename = 'Profile Class'
            where mp.meterpointtype = 'E') portfolio_register_types
               left join (select * from ref_d18_igloo_bpp union select * from ref_d18_igloo_bpp_forecast) bpp_all
                         on bpp_all.gsp_group_id = portfolio_register_types.gsp and
                            bpp_all.pcl_id::int = portfolio_register_types.pcl::int and
                            bpp_all.pfl_id = 1
               left join (select * from ref_d18_igloo_ppc union select * from ref_d18_igloo_ppc_forecast) ppc_all
                         on ppc_all.gsp_group_id = portfolio_register_types.gsp and
                            ppc_all.ss_conf_id = portfolio_register_types.ssc and
                            ppc_all.time_pattern_regime::bigint = portfolio_register_types.tpr and
                            ppc_all.pcl_id::int = portfolio_register_types.pcl::int and
                            trunc(ppc_all.st_date::timestamp) = trunc(bpp_all.st_date::timestamp)
     ) igl_ppc
where tpr is not null
order by st_date, gsp, ssc, tpr, pcl
;