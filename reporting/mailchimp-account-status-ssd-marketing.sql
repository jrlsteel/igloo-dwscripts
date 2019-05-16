select * from aws_s3_stage1_extracts.stage1_jstemp_tim_20190517
where col2 not in (select su.col2
from aws_s3_stage1_extracts.stage1_jstemp_tim_20190517 su
       inner  join ref_meterpoints mp on mp.account_id = su.col2 and mp.meterpointtype = 'E'
       inner  join ref_account_status ac on mp.account_id = ac.account_id
       inner join ref_cdb_supply_contracts sut on su.col2 = sut.external_id
       inner join ref_cdb_user_permissions rcup on sut.id = rcup.permissionable_id and permission_level = 0 and
                                                   permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_users rcu on rcup.user_id = rcu.id
       inner join ref_cdb_registrations reg on sut.registration_id
where su.col0 <> 'Christopher Williams'
group by su.col2,  ac.status
order by su.col2);



select su.col2 as account_id, reg.marketing, su.col0 as fullname, rcu.first_name, rcu.last_name, rcu.email, ac.status, min(mp.supplystartdate) as supplystartdate
from aws_s3_stage1_extracts.stage1_jstemp_tim_20190517 su
       inner  join ref_meterpoints mp on mp.account_id = su.col2 and mp.meterpointtype = 'E'
       inner  join ref_account_status ac on mp.account_id = ac.account_id
       inner join ref_cdb_supply_contracts sut on su.col2 = sut.external_id
       inner join ref_cdb_user_permissions rcup on sut.id = rcup.permissionable_id and permission_level = 0 and
                                                   permissionable_type = 'App\\SupplyContract'
       inner join ref_cdb_users rcu on rcup.user_id = rcu.id
       inner join ref_cdb_registrations reg on sut.registration_id = reg.id
where su.col0 <> 'Christopher Williams'
group by su.col2, su.col0 , rcu.first_name, rcu.last_name, rcu.email, ac.status, reg.marketing
order by su.col2;
