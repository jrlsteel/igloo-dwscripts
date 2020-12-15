create or replace view vw_smart_device_id_mpxn_map as
select distinct upper(dspinventory_esme_deviceid) as device_id,
                dspinventory_esme_importmpxn      as mpxn,
                'E'                               as fuel
from aws_smart_stage2_extracts.smart_stage2_smartreads_inventory
union
select distinct upper(dspinventory_gpf_deviceid) as deviceid,
                dspinventory_gsme_importmpxn     as mpxn,
                'G'                              as fuel
from aws_smart_stage2_extracts.smart_stage2_smartreads_inventory
with no schema binding;
alter table vw_smart_device_id_mpxn_map
    owner to igloo;