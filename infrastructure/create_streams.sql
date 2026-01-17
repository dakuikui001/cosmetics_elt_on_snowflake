/********************************************************************************
  文件：infrastructure/create_streams.sql
  更新：
    - 统一对象命名，引用 STAGE_TRIGGER_COSMETICS_DB_DEV
    - 确保路径和数据库名匹配：COSMETICS_DB_DEV
********************************************************************************/

-- 1. 切换至正确的上下文
-- 确保和 Terraform 以及 Python 里的命名完全一致
USE DATABASE COSMETICS_DB_DEV;
USE SCHEMA COSMETICS;

-- 2. 创建文件格式 (用于 Bronze 层的加载变换)
-- 保持不变，这是新增的逻辑对象
CREATE OR REPLACE FILE FORMAT BZ_CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    NULL_IF = ('NULL', '');

-- 3. 创建获取 ARN 用的占位 Pipe
-- 🔴 修正：FROM 处的 Stage 名字改成了 Terraform 里定义的新名字
CREATE OR REPLACE PIPE GET_ARN_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO COSMETICS_DB_DEV.COSMETICS.DATA_QUALITY_QUARANTINE
  FROM @COSMETICS_DB_DEV.COSMETICS.STAGE_TRIGGER_COSMETICS_DB_DEV;

-- 4. 强制刷新 Stage 的目录设置
-- 🔴 修正：Stage 名字必须改为 STAGE_TRIGGER_COSMETICS_DB_DEV
-- 这一步非常重要，它激活了 S3 到 Snowflake 的自动感应
ALTER STAGE COSMETICS_DB_DEV.COSMETICS.STAGE_TRIGGER_COSMETICS_DB_DEV 
SET DIRECTORY = (ENABLE = TRUE, AUTO_REFRESH = TRUE);

-- 5. 部署状态检查
SELECT 'SUCCESS' as STATUS, 'Infrastructure SQL components deployed.' as MESSAGE;

-- 💡 部署后手动操作提示：
-- 1. 在 Snowflake 运行：SHOW PIPES LIKE 'GET_ARN_PIPE';
-- 2. 复制 notification_channel 列的值 (一个 SQS ARN)。
-- 3. 前往 AWS S3 控制台 -> bucket-for-snowflake-projects -> Properties -> Event Notifications。
-- 4. 创建通知，目标选 SQS，填入上面复制的 ARN。