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
  # 彻底解决 Account 警告：从 ACCOUNT 变量中提取组织名和账号名
  # 假设你的格式是 "ORG-ACCOUNT"
  organization_name = split("-", var.snowflake_account)[0]
  account_name      = split("-", var.snowflake_account)[1]
  user              = var.snowflake_user
  private_key       = var.snowflake_private_key
  role              = "ACCOUNTADMIN"
}