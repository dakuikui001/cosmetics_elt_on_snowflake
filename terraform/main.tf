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

# 3. 外部卷
resource "snowflake_external_volume" "cosmetics_volume" {
  name = "COSMETICS_S3_VOLUME"
  
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
}

# 7. 修正：使用全称标识符拼接 (Database.Schema.Stage)
resource "snowflake_stream_on_directory_table" "trigger_stream" {
  name     = "TRIGGER_S3_FILE_STREAM"
  database = snowflake_database.cosmetics_db.name
  schema   = snowflake_schema.cosmetics_schema.name
  
  # 关键修改：拼接全路径
  stage    = "\"${snowflake_database.cosmetics_db.name}\".\"${snowflake_schema.cosmetics_schema.name}\".\"${snowflake_stage.trigger_stage.name}\""
  
  comment  = "Stream to monitor new files in the raw stage"
}

# 8. Pipe
resource "snowflake_pipe" "arn_pipe" {
  name     = "GET_ARN_PIPE"
  database = snowflake_database.cosmetics_db.name
  schema   = snowflake_schema.cosmetics_schema.name
  
  # 同样确保 COPY 语句里是全路径
  copy_statement = "COPY INTO \"${snowflake_database.cosmetics_db.name}\".\"${snowflake_schema.cosmetics_schema.name}\".DATA_QUALITY_QUARANTINE FROM @\"${snowflake_database.cosmetics_db.name}\".\"${snowflake_schema.cosmetics_schema.name}\".\"${snowflake_stage.trigger_stage.name}\""
  auto_ingest    = true
}