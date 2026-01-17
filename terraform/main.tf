# ==========================================
# 1. 数据库定义
# ==========================================
resource "snowflake_database" "cosmetics_db" {
  name = "COSMETICS_DB_DEV"
}

# ==========================================
# 2. 存储集成
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
# 3. 外部卷 (Iceberg Volume)
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
# 4. 架构 (Schema)
# ==========================================
resource "snowflake_schema" "cosmetics_schema" {
  database = snowflake_database.cosmetics_db.name
  name     = "COSMETICS"

  # 保护数据，防止误删
  lifecycle {
    prevent_destroy = true
  }
}

# ==========================================
# 5. 阶段 (Stages)
# ==========================================
resource "snowflake_stage" "cosmetics_s3_stage" {
  name                = "COSMETICS_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
  storage_integration = snowflake_storage_integration.s3_int.name
}

resource "snowflake_stage" "trigger_stage" {
  name                = "COSMETICS_TRIGGER_S3_STAGE"
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/"
  storage_integration = snowflake_storage_integration.s3_int.name
  directory           = "ENABLE = TRUE AUTO_REFRESH = TRUE"
}

# ==========================================
# 6. 流 (Stream)
# ==========================================
resource "snowflake_stream_on_directory_table" "trigger_stream" {
  name     = "TRIGGER_S3_FILE_STREAM"
  database = snowflake_database.cosmetics_db.name
  schema   = snowflake_schema.cosmetics_schema.name
  
  # 全称路径引用
  stage    = "\"${snowflake_database.cosmetics_db.name}\".\"${snowflake_schema.cosmetics_schema.name}\".\"${snowflake_stage.trigger_stage.name}\""
  
  comment  = "Stream to monitor new files in the raw stage"
}