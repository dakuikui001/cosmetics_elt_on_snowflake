# ==========================================
# 1. 数据库定义
# ==========================================
resource "snowflake_database" "cosmetics_db" {
  name = "COSMETICS_DB_DEV"
}

# ==========================================
# 2. 存储集成 (S3 访问权限)
# ==========================================
resource "snowflake_storage_integration" "s3_int" {
  name                      = "S3_INT_NEW"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = "arn:aws:iam::040591921557:role/snowflake_access_role_new"
  storage_allowed_locations = ["s3://bucket-for-snowflake-projects/cosmetics_etl_project/"]
}

# ==========================================
# 3. 外部卷 (Iceberg 存储配置)
# ==========================================
resource "snowflake_external_volume" "cosmetics_volume" {
  name = "COSMETICS_S3_VOLUME"
  
  storage_location {
    storage_location_name = "my-s3-cosmetics"
    storage_provider      = "S3"
    storage_base_url      = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
    storage_aws_role_arn  = "arn:aws:iam::040591921557:role/snowflake_access_role_new-volume"
  }
}

# ==========================================
# 4. 架构定义 (Schema)
# ==========================================
resource "snowflake_schema" "cosmetics_schema" {
  database = snowflake_database.cosmetics_db.name
  name     = "COSMETICS"

  # 显式声明属性，防止与云端现有状态冲突触发 destroy/replace
  data_retention_time_in_days = 1
  is_transient               = false
  with_managed_access        = false

  lifecycle {
    prevent_destroy = true # 严禁删除，保护 Schema 下的数据
  }
}

# ==========================================
# 5. 阶段 (Stages)
# ==========================================
# 主 Stage (存放代码、GE 配置和处理后的数据)
resource "snowflake_stage" "cosmetics_s3_stage" {
  name                = "COSMETICS_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
  storage_integration = snowflake_storage_integration.s3_int.name

  lifecycle {
    prevent_destroy = true
  }
}

# 触发器 Stage (专门监控 raw 文件夹)
resource "snowflake_stage" "trigger_stage" {
  name                = "COSMETICS_TRIGGER_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/"
  storage_integration = snowflake_storage_integration.s3_int.name
  directory           = "ENABLE = TRUE AUTO_REFRESH = TRUE"

  lifecycle {
    prevent_destroy = true
  }
}

# ==========================================
# 6. 流 (Stream)
# ==========================================
resource "snowflake_stream_on_directory_table" "trigger_stream" {
  name     = "TRIGGER_S3_FILE_STREAM"
  database = snowflake_database.cosmetics_db.name
  schema   = snowflake_schema.cosmetics_schema.name
  
  # 使用全称引用，适配最新的标识符要求
  stage    = "\"${snowflake_database.cosmetics_db.name}\".\"${snowflake_schema.cosmetics_schema.name}\".\"${snowflake_stage.trigger_stage.name}\""
  
  comment  = "Stream to monitor new files in the raw stage"
}