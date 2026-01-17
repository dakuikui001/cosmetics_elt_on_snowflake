resource "snowflake_database" "cosmetics_db" {
  name = "COSMETICS_DB_DEV"
}

resource "snowflake_schema" "cosmetics_schema" {
  database = snowflake_database.cosmetics_db.name
  name     = "COSMETICS"
  is_transient        = false
  with_managed_access = false
  data_retention_time_in_days = 1
}