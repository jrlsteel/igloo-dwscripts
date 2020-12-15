unload ('select * FROM ref_smart_inventory_raw')
    to 's3://igloo-data-warehouse-preprod-835569423516/tempInventory/raw/smart_inv_'
    iam_role 'arn:aws:iam::835569423516:role/CDWRedshiftClusterRole'
    HEADER
    ALLOWOVERWRITE
    parallel off
ESCAPE ;


unload ('select * FROM ref_smart_inventory_audit')
    to 's3://igloo-data-warehouse-preprod-835569423516/tempInventory/audit/smart_inv_'
    iam_role 'arn:aws:iam::835569423516:role/CDWRedshiftClusterRole'
    HEADER
    ALLOWOVERWRITE
    parallel off
ESCAPE ;

unload ('select * FROM ref_smart_inventory')
    to 's3://igloo-data-warehouse-preprod-835569423516/tempInventory/inv/smart_inv_'
    iam_role 'arn:aws:iam::835569423516:role/CDWRedshiftClusterRole'
    HEADER
    ALLOWOVERWRITE
    parallel off
ESCAPE ;
