# 定义变量（这些变量会接收 GitHub Secrets 的值）
variable "snowflake_account" { type = string }
variable "snowflake_user"    { type = string }
variable "snowflake_private_key" { type = string }

terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.87"
    }
  }
}

provider "snowflake" {
  organization_name = "TMJESFF"
  account_name      = "ED32433"
  user              = var.snowflake_user
  private_key       = var.snowflake_private_key # 关键：改用私钥
  role              = "ACCOUNTADMIN"
}