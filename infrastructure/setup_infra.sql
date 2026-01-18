/********************************************************************************
  文件：infrastructure/setup_infra.sql
  功能：一键创建所有物理通道（Integration, Volume, Stage, Pipe）
  更新：
    - 将 Integration 和 Volume 改为 IF NOT EXISTS，防止 AWS 身份 ID 重置
    - Stage 和 Pipe 保持 OR REPLACE 以支持逻辑更新
********************************************************************************/

-- 1. 环境初始化 (前提：Terraform 已创建 DB 和 SCHEMA)
USE DATABASE COSMETICS_DB_DEV;
USE SCHEMA COSMETICS; 

-- 2. 创建存储集成 (与 S3 通信的 IAM 凭证)
-- 🔴 关键修正：使用 IF NOT EXISTS。
-- 这样只有在对象不存在时才创建。如果已存在，它将保持原有的 STORAGE_AWS_EXTERNAL_ID，
-- 避免你每次都要去 AWS 控制台更新信任关系。
CREATE STORAGE INTEGRATION IF NOT EXISTS INT_S3_COSMETICS_DB_DEV
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::040591921557:role/snowflake_access_role_new'
  STORAGE_ALLOWED_LOCATIONS = ('s3://bucket-for-snowflake-projects/cosmetics_etl_project/');

-- 3. 创建 Iceberg 外部卷 (Iceberg 表写数据的物理位置)
-- 🔴 关键修正：使用 IF NOT EXISTS。
-- 防止重新生成 External ID，确保 Iceberg 表始终有权限写入 S3。
CREATE EXTERNAL VOLUME IF NOT EXISTS VOL_S3_COSMETICS_DB_DEV
   STORAGE_LOCATIONS = (
      (
         NAME = 'loc_cosmetics_dev'
         STORAGE_PROVIDER = 'S3'
         STORAGE_BASE_URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/'
         STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::040591921557:role/snowflake_access_role_new-volume'
      )
   );

-- 4. 创建外部 Stage (用于 COPY INTO 数据加载)
-- Stage 可以保持 OR REPLACE，因为它引用的是 Integration 的名称，不会改变底层权限身份。
CREATE OR REPLACE STAGE STAGE_COSMETICS_DB_DEV
  URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/'
  STORAGE_INTEGRATION = INT_S3_COSMETICS_DB_DEV;

-- 5. 创建带目录功能的触发 Stage (用于监听 raw/ 目录)
CREATE OR REPLACE STAGE STAGE_TRIGGER_COSMETICS_DB_DEV
  URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/'
  STORAGE_INTEGRATION = INT_S3_COSMETICS_DB_DEV
  DIRECTORY = (ENABLE = TRUE, AUTO_REFRESH = TRUE);

-- 6. 创建目录表 Stream (监听文件到达)
CREATE OR REPLACE STREAM STREAM_TRIGGER_COSMETICS_DB_DEV
  ON STAGE STAGE_TRIGGER_COSMETICS_DB_DEV;

-- 7. 创建文件格式
CREATE OR REPLACE FILE FORMAT BZ_CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    NULL_IF = ('NULL', '');

-- 8. 创建独立占位表与 Pipe
-- 确保 Pipe 目标表始终存在
CREATE TABLE IF NOT EXISTS COSMETICS_DB_DEV.COSMETICS.STG_PIPE_PLACEHOLDER (
    raw_content VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 创建或替换 Pipe，目标指向占位表  
CREATE OR REPLACE PIPE GET_ARN_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO COSMETICS_DB_DEV.COSMETICS.STG_PIPE_PLACEHOLDER
  FROM @STAGE_TRIGGER_COSMETICS_DB_DEV;

-- 部署确认
SELECT 'SUCCESS' as STATUS, 'Infrastructure deployed. Integration/Volume preserved if existed.' as MESSAGE;