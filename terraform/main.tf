resource "snowflake_database" "cosmetics_db" {
  name = "COSMETICS_DB_DEV"
}

resource "snowflake_schema" "cosmetics_schema" {
  database = snowflake_database.cosmetics_db.name
  name     = "COSMETICS"
}