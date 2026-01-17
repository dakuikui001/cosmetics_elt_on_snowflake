terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.87" 
    }
  }
}

provider "snowflake" {
  role = "ACCOUNTADMIN"
}