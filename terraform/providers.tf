variable "snowflake_org" {}
variable "snowflake_account" {}
variable "snowflake_user" {}
variable "snowflake_private_key" {}

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.87"
    }
  }
}

provider "snowflake" {
  organization_name = var.snowflake_org
  account_name      = var.snowflake_account
  user              = var.snowflake_user
  private_key       = var.snowflake_private_key
  role              = "ACCOUNTADMIN"
}