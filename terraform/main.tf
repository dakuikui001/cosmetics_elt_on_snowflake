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

# 3. 外部卷 (修复权限与路径匹配)
resource "snowflake_external_volume" "cosmetics_volume" {
  name         = "COSMETICS_S3_VOLUME"
  allow_writes = "true"

  storage_location {
    storage_location_name = "my-s3-cosmetics"
    storage_provider      = "S3"
    storage_base_url      = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
    storage_aws_role_arn  = "arn:aws:iam::040591921557:role/snowflake_access_role_new-volume"
  }
}

# 4. 架构 (强制对齐云端所有默认参数，防止 Replacement)
resource "snowflake_schema" "cosmetics_schema" {
  database = snowflake_database.cosmetics_db.name
  name     = "COSMETICS"

  # 关键修复：显式匹配云端抛出的差异参数
  is_transient                  = false
  with_managed_access           = false
  data_retention_time_in_days   = 1
  max_data_extension_time_in_days = 14
  
  lifecycle {
    prevent_destroy = true
    # 终极保险：忽略任何其他由 Snowflake 自动生成的微小参数变动
    ignore_changes = [
      parameters,
      storage_serialization_policy,
      suspend_task_after_num_failures,
      task_auto_retry_attempts,
      trace_level,
      user_task_managed_initial_warehouse_size,
      user_task_minimum_trigger_interval_in_seconds,
      user_task_timeout_ms
    ]
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

# 6. 触发器 Stage
resource "snowflake_stage" "trigger_stage" {
  name                = "COSMETICS_TRIGGER_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/"
  storage_integration = snowflake_storage_integration.s3_int.name
  directory           = "ENABLE = TRUE AUTO_REFRESH = TRUE"

  lifecycle {
    ignore_changes = [directory]
  }
}

# 7. 触发器 Stream
resource "snowflake_stream_on_directory_table" "trigger_stream" {
  name     = "TRIGGER_S3_FILE_STREAM"
  database = snowflake_database.cosmetics_db.name
  schema   = snowflake_schema.cosmetics_schema.name
  stage    = "\"${snowflake_database.cosmetics_db.name}\".\"${snowflake_schema.cosmetics_schema.name}\".\"${snowflake_stage.trigger_stage.name}\""
}