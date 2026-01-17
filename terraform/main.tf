# 1. 创建数据库
resource "snowflake_database" "cosmetics_db" {
  name = "COSMETICS_DB_DEV"
}

# 2. 创建存储集成 (使用刚才拿到的 AWS ARN)
resource "snowflake_storage_integration" "s3_int" {
  name    = "S3_INT_NEW"
  type    = "EXTERNAL_STAGE"
  enabled = true
  storage_provider          = "S3"
  storage_aws_role_arn      = "arn:aws:iam::040591921557:role/snowflake_access_role_new"
  storage_allowed_locations = ["s3://bucket-for-snowflake-projects/cosmetics_etl_project/"]
}