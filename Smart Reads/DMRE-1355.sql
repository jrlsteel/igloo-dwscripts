create table if not exists ref_readings_smart_daily_audit
(
	mpxn bigint distkey ,
    deviceid varchar(50)  ,
    type varchar(50)  ,
    total_consumption int ,
    register_num int,
    register_value int,
    timestamp timestamp  ,
    partition_date timestamp,
	etlchangetype varchar(1),
	etlchange timestamp
)
diststyle key
sortkey(mpxn, deviceid,type, timestamp )
;


alter table ref_readings_smart_daily_audit owner to igloo
;



create table if not exists ref_readings_smart_daily
(
	mpxn bigint distkey,
    deviceid varchar(50) ,
    type varchar(50) ,
    total_consumption int ,
    register_num int,
    register_value int,
    timestamp timestamp ,
    partition_date timestamp,
    etlchange timestamp
)
diststyle key
sortkey(mpxn, deviceid,type, timestamp )
;


alter table ref_readings_smart_daily owner to igloo
;