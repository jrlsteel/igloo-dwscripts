select  account_id,
        meter_point_id,
        meterpointnumber,
        meterpointtype,
        supplystartdate,
        supplyenddate,
        max(GSP) as GSP,
        max(Supply_Status) as Supply_Status,
        max(MeterType)as MeterType
FROM(
select  mp.account_id,
        mp.meter_point_id,
        mp.meterpointnumber,
        mp.meterpointtype,
        mp.supplystartdate,
        mp.supplyenddate,
        case when attributes_attributename ='GSP' then mpa.attributes_attributevalue end as GSP,
        case when attributes_attributename ='Supply_Status' then mpa.attributes_attributevalue end as Supply_Status,
        case when attributes_attributename ='MeterType' then mpa.attributes_attributevalue end as MeterType
      --listagg(distinct(mpa.attributes_attributevalue),',') within group (order by mpa.attributes_attributename desc)
    from aws_s3_ensec_api_extracts.cdb_stagemeterpoints mp,
         aws_s3_ensec_api_extracts.cdb_stagemeterpointsattributes mpa
WHERE mp.account_id = mpa.account_id
AND   (mpa.attributes_attributename = 'MeterType'
           or mpa.attributes_attributename = 'GSP'
           or (mpa.attributes_attributename = 'Supply_Status'
                 AND lower(mpa.attributes_attributevalue) = 'live'))
AND   mp.supplyenddate is null
GROUP BY  mp.account_id,
        mp.meter_point_id,
        mp.meterpointnumber,
        mp.meterpointtype,
        mp.supplystartdate,
        mp.supplyenddate,
        mpa.attributes_attributename,
        mpa.attributes_attributevalue)
GROUP BY account_id,
        meter_point_id,
        meterpointnumber,
        meterpointtype,
        supplystartdate,
        supplyenddate
ORDER BY account_id,
        meter_point_id,
        meterpointnumber,
        meterpointtype,
        supplystartdate,
        supplyenddate