


create view meets_group_counts as
select case
           when dcf.eac_igloo_ca <= 3350 then 'S'
           when dcf.eac_igloo_ca <= 5161 then 'M'
           else 'L' end               as eac_band,
       dcf.gsp,
       sum(mpd.`group` = 'Control')   as num_control,
       sum(mpd.`group` = 'Treatment') as num_treatment,
       count(*)                       as total
from meets_project_data mpd
         inner join users u on mpd.user_id = u.id
         inner join user_permissions up on u.id = up.user_id and
                                           up.permissionable_type = 'App\\SupplyContract' and
                                           up.permission_level = 0
         inner join supply_contracts sc
                    on up.permissionable_id = sc.id and mpd.address_id = sc.supply_address_id
         left join igloo_datawarehouse.ref_calculated_daily_customer_file dcf
                   on sc.external_id = dcf.account_id
where mpd.opted_out is null
group by gsp, eac_band;


create view meets_account_strata as
select account_id,
       gsp,
       eac_igloo_ca,
       case
           when eac_igloo_ca <= 3350 then 'S'
           when eac_igloo_ca <= 5161 then 'M'
           else 'L' end as eac_band
from igloo_datawarehouse.ref_calculated_daily_customer_file;


create view meets_signup_groups as
select dcf_signup.*,
       coalesce(existing_counts.num_control, 0)   as existing_control,
       coalesce(existing_counts.num_treatment, 0) as existing_treatment,
       case sign(coalesce((existing_counts.num_control - existing_counts.num_treatment), 0) +
                 (dcf_signup.account_id % 2) - 0.5) # this is a tie-break for if num_control = num_treatment

           when -1 then 'Control'
           else 'Treatment' end                   as assigned_group
from meets_account_strata dcf_signup
         left join meets_group_counts as existing_counts on dcf_signup.gsp = existing_counts.gsp and
                                                            dcf_signup.eac_band = existing_counts.eac_band
order by account_id;

# select *
# from meets_signup_groups
# where account_id = 54977


### TESTING ###

select account_id, assigned_group
from meets_signup_groups
where existing_control = 0 and existing_treatment = 0

select distinct assigned_group
from meets_signup_groups
where existing_control > existing_treatment

select distinct assigned_group
from meets_signup_groups
where existing_control < existing_treatment

select sum(assigned_group = 'Control') as num_control,
       sum(assigned_group = 'Treatment') as num_treat
from meets_signup_groups
where existing_control = existing_treatment