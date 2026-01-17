/********************************************************************************
  文件：infrastructure/setup_infra.sql
  功能：一键创建所有物理通道（Integration, Volume, Stage, Pipe）
  特点：使用 CREATE OR REPLACE，解决 Terraform 状态冲突问题
********************************************************************************/

-- 1. 环境初始化 (前提：Terraform 已创建 DB 和 SCHEMA)
USE DATABASE COSMETICS_DB_DEV;
USE SCHEMA COSMETICS;

-- 2. 创建存储集成 (与 S3 通信的 IAM 凭证)
-- 如果此对象已存在且正在被 Stage 使用，OR REPLACE 可能会有短暂抖动，但在开发环境没问题
CREATE OR REPLACE STORAGE INTEGRATION INT_S3_COSMETICS_DB_DEV
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::040591921557:role/snowflake_access_role_new'
  STORAGE_ALLOWED_LOCATIONS = ('s3://bucket-for-snowflake-projects/cosmetics_etl_project/');

-- 3. 创建 Iceberg 外部卷 (Iceberg 表写数据的物理位置)
CREATE OR REPLACE EXTERNAL VOLUME VOL_S3_COSMETICS_DB_DEV
   STORAGE_LOCATIONS = (
      (
         NAME = 'loc_cosmetics_dev'
         STORAGE_PROVIDER = 'S3'
         STORAGE_BASE_URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/'
         STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::040591921557:role/snowflake_access_role_new-volume'
      )
   );

-- 4. 创建外部 Stage (用于 COPY INTO 数据加载)
CREATE OR REPLACE STAGE STAGE_COSMETICS_DB_DEV
  URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/'
  STORAGE_INTEGRATION = INT_S3_COSMETICS_DB_DEV;

-- 5. 创建带目录功能的触发 Stage (用于监听 raw/ 目录)
CREATE OR REPLACE STAGE STAGE_TRIGGER_COSMETICS_DB_DEV
  URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/'
  STORAGE_INTEGRATION = INT_S3_COSMETICS_DB_DEV
  DIRECTORY = (ENABLE = TRUE, AUTO_REFRESH = TRUE);

-- 6. 创建目录表 Stream (监听文件到达)
-- 注意：这是为了替代 Terraform 中被删除的 snowflake_stream_on_directory_table
CREATE OR REPLACE STREAM STREAM_TRIGGER_COSMETICS_DB_DEV
  ON STAGE STAGE_TRIGGER_COSMETICS_DB_DEV;

-- 7. 创建文件格式
CREATE OR REPLACE FILE FORMAT BZ_CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    NULL_IF = ('NULL', '');

-- 8. 创建独立占位表与 Pipe
-- 使用独立表名，避免与 Python 脚本中的 Iceberg 表冲突
CREATE TABLE IF NOT EXISTS COSMETICS_DB_DEV.COSMETICS.STG_PIPE_PLACEHOLDER (
    raw_content VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 创建或替换 Pipe，目标指向占位表
-- 这样即使 Python 还在跑，Pipe 的创建也不会报错
CREATE OR REPLACE PIPE GET_ARN_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO COSMETICS_DB_DEV.COSMETICS.STG_PIPE_PLACEHOLDER
  FROM @STAGE_TRIGGER_COSMETICS_DB_DEV;

-- 部署确认
SELECT 'SUCCESS' as STATUS, 'All physical infrastructure (Integration, Volume, Stage, Stream) deployed.' as MESSAGE; 