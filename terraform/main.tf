# 1. 数据库
resource "snowflake_database" "cosmetics_db" {
  name = "COSMETICS_DB_DEV"
}

# 2. 存储集成
resource "snowflake_storage_integration" "s3_int" {
  name                      = "S3_INT_NEW"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = "arn:aws:iam::040591921557:role/snowflake_access_role_new"
  storage_allowed_locations = ["s3://bucket-for-snowflake-projects/cosmetics_etl_project/"]
}

# 3. 外部卷 (修复权限变更报错)
resource "snowflake_external_volume" "cosmetics_volume" {
  name         = "COSMETICS_S3_VOLUME"
  allow_writes = "true" # 强制匹配云端权限，防止 Snowflake 拒绝修改已绑定的 Iceberg 卷

  storage_location {
    storage_location_name = "my-s3-cosmetics"
    storage_provider      = "S3"
    storage_base_url      = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
    storage_aws_role_arn  = "arn:aws:iam::040591921557:role/snowflake_access_role_new-volume"
  }
}

# 4. 架构
resource "snowflake_schema" "cosmetics_schema" {
  database = snowflake_database.cosmetics_db.name
  name     = "COSMETICS"

  lifecycle {
    prevent_destroy = true
  }
}

# 5. 主 Stage
resource "snowflake_stage" "cosmetics_s3_stage" {
  name                = "COSMETICS_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
  storage_integration = snowflake_storage_integration.s3_int.name
}

# 6. 触发器 Stage (通过 ignore_changes 防止因 SQS 通知导致的销毁重建)
resource "snowflake_stage" "trigger_stage" {
  name                = "COSMETICS_TRIGGER_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/"
  storage_integration = snowflake_storage_integration.s3_int.name
  directory           = "ENABLE = TRUE AUTO_REFRESH = TRUE"

  lifecycle {
    # 强制忽略 directory 的变更，因为导入时云端自动生成的 SQS ARN 无法在代码中完全匹配
    ignore_changes = [directory]
  }
}

# 7. 触发器 Stream
resource "snowflake_stream_on_directory_table" "trigger_stream" {
  name     = "TRIGGER_S3_FILE_STREAM"
  database = snowflake_database.cosmetics_db.name
  schema   = snowflake_schema.cosmetics_schema.name
  
  stage    = "\"${snowflake_database.cosmetics_db.name}\".\"${snowflake_schema.cosmetics_schema.name}\".\"${snowflake_stage.trigger_stage.name}\""
  
  comment  = "Stream to monitor new files in the raw stage"
}