# --- 1. 数据库与 Schema ---
resource "snowflake_database" "cosmetics_db" {
  name = "COSMETICS_DB_DEV"
}

resource "snowflake_schema" "cosmetics_schema" {
  database = snowflake_database.cosmetics_db.name
  name     = "COSMETICS"
}

# --- 2. 存储集成 (命名关联数据库) ---
resource "snowflake_storage_integration" "s3_int" {
  name                      = "INT_S3_COSMETICS_DB_DEV" # 重新起名
  type                      = "EXTERNAL_STAGE"
  enabled                   = true
  storage_provider          = "S3"
  storage_aws_role_arn      = "arn:aws:iam::040591921557:role/snowflake_access_role_new"
  storage_allowed_locations = ["s3://bucket-for-snowflake-projects/cosmetics_etl_project/"]
}

# --- 3. Iceberg 外部卷 (命名关联数据库，去除生命周期限制) ---
resource "snowflake_external_volume" "cosmetics_volume" {
  name         = "VOL_S3_COSMETICS_DB_DEV" # 重新起名
  allow_writes = "true"
  storage_location {
    storage_location_name = "loc_cosmetics_dev"
    storage_provider      = "S3"
    storage_base_url      = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
    storage_aws_role_arn  = "arn:aws:iam::040591921557:role/snowflake_access_role_new-volume"
  }
  # 彻底删除 ignore_changes，确保 Terraform 拥有绝对路径控制权
}

# --- 4. 外部 Stage ---
resource "snowflake_stage" "cosmetics_s3_stage" {
  name                = "STAGE_COSMETICS_DB_DEV" # 重新起名
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/"
  storage_integration = snowflake_storage_integration.s3_int.name
}

resource "snowflake_stage" "trigger_stage" {
  name                = "STAGE_TRIGGER_COSMETICS_DB_DEV" # 重新起名
  database            = snowflake_database.cosmetics_db.name
  schema              = snowflake_schema.cosmetics_schema.name
  url                 = "s3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/"
  storage_integration = snowflake_storage_integration.s3_int.name
  directory           = "ENABLE = TRUE AUTO_REFRESH = TRUE"
} 

# --- 5. Stream ---
resource "snowflake_stream_on_directory_table" "trigger_stream" {
  name     = "STREAM_TRIGGER_COSMETICS_DB_DEV" # 重新起名
  database = snowflake_database.cosmetics_db.name
  schema   = snowflake_schema.cosmetics_schema.name
  stage    = "\"${snowflake_database.cosmetics_db.name}\".\"${snowflake_schema.cosmetics_schema.name}\".\"${snowflake_stage.trigger_stage.name}\""
}