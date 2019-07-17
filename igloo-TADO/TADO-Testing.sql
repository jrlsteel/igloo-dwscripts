select *
from ref_readings_internal_valid
where account_id in
      (46456, 46496, 46429, 46389, 46344, 46347, 46349, 46322, 46342, 46296, 46309, 46248, 46267, 46280, 46255, 46257, 46254, 46279, 46271, 46231, 46220, 46225, 46176, 46180, 46178, 46128, 46114, 46122, 46125, 46139, 46138, 46140, 46149, 46032, 46054, 45992, 45985, 46057, 45997, 46053, 46063, 46062, 45990, 46045, 46029, 46028, 46040, 46003, 46041, 46015, 46008, 45928, 45927, 45933, 45893, 45895, 45909, 45896, 45882, 45886, 45894, 45812, 45816, 45828, 45857, 45786, 45736, 45764, 45742, 45756, 45722, 45774, 45729, 45713, 45673, 45661, 45670, 45672, 45663, 45566, 45547, 45549, 45569, 45571, 45517, 45494, 45475, 45510, 45453, 45493, 45512, 45477, 45449, 45428, 45448, 45454, 45462, 45446, 45450, 45491, 45503, 45496, 45457, 45337, 45394, 45402, 45353, 45375, 45397, 45369, 45362, 45365, 45383, 45380, 45331, 45399, 45350, 43353, 45390, 45418, 45386, 45413, 45282, 45204, 45290, 45264, 45258, 45232, 45190, 45287, 45183, 45288, 45140, 45223, 45121, 45279, 45182, 45301, 45209, 45297, 45202, 45310, 45181, 45208, 45214, 45274, 45114, 45160, 45295, 45158, 45268, 45218, 45151, 45188, 45177, 45296, 43981, 45059, 44022, 44015, 44033, 44016, 45047, 43994, 44040, 44039, 45075, 44035, 45072, 44044, 44025, 44024, 44002, 45050, 43948, 43943, 43887, 43882, 43933, 43938, 43907, 43921, 43875, 43859, 43894, 43880, 43900, 43929, 43931, 43904, 43858, 43926, 43893, 43899, 43917, 43930, 43932, 43902, 43855, 43920)
and meterreadingdatetime > '2019-07-15';



select *
from ref_calculated_aq
where account_id in
      (select account_id
       from ref_readings_internal_valid
       where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by ref_calculated_aq.account_id);

select *
from ref_calculated_aq_audit
where account_id in
      (select account_id
       from ref_readings_internal_valid
           where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id)and etlchangetype = 'u';

select *
from ref_calculated_igl_ind_aq
where account_id in
      (select account_id
       from ref_readings_internal_valid
       where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id);

select *
from ref_calculated_igl_ind_aq
where account_id in
      (select account_id
       from ref_readings_internal_valid
       where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id)
and account_id = 4774;

select *
from ref_calculated_igl_ind_aq_audit
where account_id in
      (select account_id
       from ref_readings_internal_valid
       where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id)
and etlchangetype = 'u';

select *
from ref_calculated_igl_ind_aq_audit
where account_id in
      (select account_id
       from ref_readings_internal_valid
       where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id)
and account_id =4474;

select *
from ref_consumption_accuracy_gas
where account_id in
      (select account_id
       from ref_readings_internal_valid
          where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id);

select *
from ref_consumption_accuracy_gas
where account_id in
      (select account_id
       from ref_readings_internal_valid
          where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id)
and account_id = 4774;

select *
from ref_consumption_accuracy_gas_audit
where account_id in
      (select account_id
       from ref_readings_internal_valid
        where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id)
and etlchangetype = 'u';

select * from public.ref_calculated_tado_efficiency_batch
where account_id in
      (select account_id
       from ref_readings_internal_valid
       where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id);

select * from public.ref_calculated_tado_efficiency_batch
where account_id in
      (select account_id
       from ref_readings_internal_valid
       where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'G'
       group by account_id)
and account_id =4474;

-- Electricity Checking Scripts

select *
from ref_calculated_eac
where account_id in
      (select account_id
       from ref_readings_internal_valid
    where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'E'
       group by ref_calculated_eac.account_id);

select *
from ref_calculated_eac_audit
where account_id in
      (select account_id
       from ref_readings_internal_valid
 where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'E'
       group by ref_calculated_eac_audit.account_id);

select *
from ref_calculated_igl_ind_eac
where account_id in
      (select account_id
       from ref_readings_internal_valid
 where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'E'
       group by ref_calculated_igl_ind_eac.account_id);

select *
from ref_calculated_igl_ind_eac_audit
where account_id in
      (select account_id
       from ref_readings_internal_valid
 where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'E'
       group by ref_calculated_igl_ind_eac_audit.account_id);

select *
from ref_consumption_accuracy_elec
where account_id in
      (select account_id
       from ref_readings_internal_valid
 where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'E'
       group by account_id);

select *
from ref_consumption_accuracy_elec_audit
where account_id in
      (select account_id
       from ref_readings_internal_valid
   where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'E'
       group by account_id);

select * from public.ref_calculated_tado_efficiency_batch
where account_id in
      (select account_id
       from ref_readings_internal_valid
    where meterreadingdatetime > '2019-07-16'
         and meterpointtype = 'E'
       group by account_id);
