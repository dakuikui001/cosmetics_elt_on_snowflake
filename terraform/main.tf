# 1. 数据库定义
resource "snowflake_database" "cosmetics_db" {
  name = "COSMETICS_DB_DEV"
}

# 2. 存储集成 (保持你定好的 S3_INT_NEW)
resource "snowflake_storage_integration" "s3_int" {
  name                      = "S3_INT_NEW"
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  # 第一个 Role: 专门用于集成/读取
  storage_aws_role_arn      = "arn:aws:iam::040591921557:role/snowflake_access_role_new"
  storage_allowed_locations = ["s3://bucket-for-snowflake-projects/cosmetics_etl_project/"]
}

# 3. 外部卷 (对应 SQL 中的 COSMETICS_S3_VOLUME)
resource "snowflake_external_volume" "cosmetics_volume" {
  name = "COSMETICS_S3_VOLUME"
  
  storage_location {
    name                = "my-s3-cosmetics"
    storage_provider    = "S3"
    storage_base_url    = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
    # 第二个 Role: 对应 SQL 里的 volume 专用角色
    storage_aws_role_arn = "arn:aws:iam::040591921557:role/snowflake_access_role_new-volume"
  }
}

# 4. 架构定义 (Schema)
resource "snowflake_schema" "cosmetics_schema" {
  database = snowflake_database.cosmetics_db.name
  name     = "COSMETICS"
}

# 5. 主 Stage (存放 Python 代码、Great Expectations 配置和处理后的数据)
resource "snowflake_stage" "cosmetics_s3_stage" {
  name                = "COSMETICS_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
  storage_integration = snowflake_storage_integration.s3_int.name
}

# 6. 触发器 Stage (专门监控 raw 文件夹)
resource "snowflake_stage" "trigger_stage" {
  name                = "COSMETICS_TRIGGER_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/"
  storage_integration = snowflake_storage_integration.s3_int.name
  
  # 开启目录服务，以便 Stream 实时捕获文件上传
  directory = "ENABLE = TRUE AUTO_REFRESH = TRUE"
}

# 7. 搬运 SQL 中的 Stream (监控 Stage 变化的开关)
resource "snowflake_stream" "trigger_stream" {
  name        = "TRIGGER_S3_FILE_STREAM"
  database    = snowflake_database.cosmetics_db.name
  schema      = snowflake_schema.cosmetics_schema.name
  on_stage    = "${snowflake_database.cosmetics_db.name}.${snowflake_schema.cosmetics_schema.name}.${snowflake_stage.trigger_stage.name}"
  append_only = true
}

# 8. 搬运 SQL 中的 Pipe (用于 DQ 隔离区的自动加载)
resource "snowflake_pipe" "arn_pipe" {
  name     = "GET_ARN_PIPE"
  database = snowflake_database.cosmetics_db.name
  schema   = snowflake_schema.cosmetics_schema.name
  
  # 这里的目标表会在 SETUPDB.ipynb 中通过 helper.setup() 创建
  copy_statement = "COPY INTO ${snowflake_database.cosmetics_db.name}.${snowflake_schema.cosmetics_schema.name}.DATA_QUALITY_QUARANTINE FROM @${snowflake_database.cosmetics_db.name}.${snowflake_schema.cosmetics_schema.name}.${snowflake_stage.trigger_stage.name}"
  auto_ingest    = true
}