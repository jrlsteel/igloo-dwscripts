create external schema aws_smart_stage2_extracts
from
database 'uat-data-catalogue-smart-stage2'
iam_role 'arn:aws:iam::630944350233:role/AmazonRedshiftRoleCustom'
;