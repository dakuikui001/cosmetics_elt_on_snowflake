/********************************************************************************
  文件：infrastructure/create_streams.sql
  描述：
    1. 配置 File Format（用于 COPY INTO 变换）
    2. 创建占位 Pipe（用于获取 AWS S3 Notification 所需的 ARN）
    3. 刷新 Stage 目录配置
  
  注意：TRIGGER_S3_FILE_STREAM 已经在 Terraform 中定义，请勿在此重复创建。
********************************************************************************/

-- 1. 切换至正确的上下文
USE DATABASE COSMETICS_DB_DEV;
USE SCHEMA COSMETICS;

-- 2. 创建文件格式 (用于 Bronze 层的加载变换)
-- Python Helper 和 Terraform 均未管理此对象，需在此定义
CREATE OR REPLACE FILE FORMAT BZ_CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    NULL_IF = ('NULL', '');

-- 3. 创建获取 ARN 用的占位 Pipe
-- 作用：执行后通过 `SHOW PIPES` 获取 notification_channel，用于 S3 事件集成
-- 此时不需要它真的去跑数据，仅仅是为了获取 S3 端的终点 ARN
CREATE OR REPLACE PIPE GET_ARN_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO COSMETICS_DB_DEV.COSMETICS.DATA_QUALITY_QUARANTINE
  FROM @COSMETICS_DB_DEV.COSMETICS.COSMETICS_TRIGGER_S3_STAGE;

-- 4. 强制刷新 Stage 的目录设置
-- 确保 Terraform 创建的目录表 Stream 能够实时感应到 S3 的文件变化
ALTER STAGE COSMETICS_DB_DEV.COSMETICS.COSMETICS_TRIGGER_S3_STAGE 
SET DIRECTORY = (ENABLE = TRUE, AUTO_REFRESH = TRUE);

-- 5. 部署状态检查
SELECT 'SUCCESS' as STATUS, 'Infrastructure SQL components deployed.' as MESSAGE;

-- 💡 部署后手动操作提示：
-- 执行下面这行，复制 "notification_channel" 这一列的内容，填入 S3 Bucket 的 Event Notification 中
-- SHOW PIPES LIKE 'GET_ARN_PIPE';