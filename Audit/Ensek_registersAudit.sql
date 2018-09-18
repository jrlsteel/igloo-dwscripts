--Updated Registers SQL
insert into ref_registers_audit
select
		s.account_id,
		s.meter_point_id,
		s.meter_id,
		s.register_id,
		s.registers_eacaq,
		s.registers_registerreference,
		s.registers_sourceidtype,
		s.registers_tariffcomponent,
		s.registers_tpr,
		s.registers_tprperioddescription,
		'u', current_timestamp
from aws_s3_ensec_api_extracts.cdb_registers s
       inner join ref_registers r
       		ON s.meter_id = r.meter_id
where (s.register_id != r.register_id
		or s.registers_eacaq != r.registers_eacaq
		or s.registers_registerreference != r.registers_registerreference
		or s.registers_sourceidtype != r.registers_sourceidtype
		or s.registers_tariffcomponent != r.registers_tariffcomponent
		or s.registers_tpr != r.registers_tpr
		or s.registers_tprperioddescription != r.registers_tprperioddescription);


--New Registers SQL
insert into ref_registers_audit
select
		s.account_id,
		s.meter_point_id,
		s.meter_id,
		s.register_id,
		s.registers_eacaq,
		s.registers_registerreference,
		s.registers_sourceidtype,
		s.registers_tariffcomponent,
		s.registers_tpr,
		s.registers_tprperioddescription,
		'n', current_timestamp
from aws_s3_ensec_api_extracts.cdb_registers s
left outer join ref_registers r
		ON r.meter_id = s.meter_id
where r.meter_id is null;


--Deleted Registers SQL
insert into ref_registers_audit
select
		r.account_id,
		r.meter_point_id,
		r.meter_id,
		r.register_id,
		r.registers_eacaq,
		r.registers_registerreference,
		r.registers_sourceidtype,
		r.registers_tariffcomponent,
		r.registers_tpr,
		r.registers_tprperioddescription,
		'd', current_timestamp
from ref_registers r
left outer join aws_s3_ensec_api_extracts.cdb_registers s
		ON r.meter_id = s.meter_id
where s.meter_id is null;