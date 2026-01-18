/********************************************************************************
  文件：orchestration/deploy_dags.sql
  功能：注册存储过程并基于物理基础设施编排 Task DAG
********************************************************************************/

-- =====================================================
-- 1. 环境上下文设置
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE COSMETICS_DB_DEV;
USE SCHEMA COSMETICS;

-- =====================================================
-- 2. 注册存储过程 (Procedures)
-- =====================================================

-- 注册 Bronze 过程 (数据清洗与质量校验)
CREATE OR REPLACE PROCEDURE RUN_BRONZE_PROC(env STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'great-expectations==1.8.0')
IMPORTS = (
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/bronze.py', 
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/great_expectations_common.py',
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/main_pipeline.py'
)
HANDLER = 'main_pipeline.run_bronze_step';

-- 注册 Silver 过程 (去重与表合并)
CREATE OR REPLACE PROCEDURE RUN_SILVER_PROC(env STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = (
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/silver.py', 
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/main_pipeline.py'
)
HANDLER = 'main_pipeline.run_silver_step';

-- 注册 Gold 过程 (指标加工与 Fact 表合并)
CREATE OR REPLACE PROCEDURE RUN_GOLD_PROC(env STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
IMPORTS = (
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/gold.py', 
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/main_pipeline.py'
)
HANDLER = 'main_pipeline.run_gold_step';

-- =====================================================
-- 3. 定义调度任务 (Tasks) - 严格对齐 setup_infra.sql
-- =====================================================

-- 根任务：监控 S3 目录 Stage 的变动 Stream
CREATE OR REPLACE TASK BRONZE_TASK
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTE'
    -- 🔴 修正：对齐 setup_infra.sql 中的 STREAM_TRIGGER_COSMETICS_DB_DEV
    WHEN SYSTEM$STREAM_HAS_DATA('COSMETICS_DB_DEV.COSMETICS.STREAM_TRIGGER_COSMETICS_DB_DEV')
AS
    CALL RUN_BRONZE_PROC('DEV');

-- Silver 任务：紧随 Bronze 成功且 Bronze 表有变更时触发
CREATE OR REPLACE TASK SILVER_TASK
    WAREHOUSE = 'COMPUTE_WH'
    AFTER BRONZE_TASK
    -- 🔴 确保此 Stream 已经在 setup_tables.py 中创建
    WHEN SYSTEM$STREAM_HAS_DATA('COSMETICS_DB_DEV.COSMETICS.COSMETICS_BZ_STREAM')
AS
    CALL RUN_SILVER_PROC('DEV');

-- Gold 任务：紧随 Silver 成功且 Silver 表有变更时触发
CREATE OR REPLACE TASK GOLD_TASK
    WAREHOUSE = 'COMPUTE_WH'
    AFTER SILVER_TASK
    -- 🔴 确保此 Stream 已经在 setup_tables.py 中创建
    WHEN SYSTEM$STREAM_HAS_DATA('COSMETICS_DB_DEV.COSMETICS.COSMETICS_SL_STREAM')
AS
    CALL RUN_GOLD_PROC('DEV');

-- =====================================================
-- 4. 激活任务流 (必须从叶子到根开启)
-- =====================================================
-- 激活前先暂停，确保更新应用
ALTER TASK IF EXISTS GOLD_TASK SUSPEND;
ALTER TASK IF EXISTS SILVER_TASK SUSPEND;
ALTER TASK IF EXISTS BRONZE_TASK SUSPEND;

-- 启动任务链
ALTER TASK GOLD_TASK RESUME;
ALTER TASK SILVER_TASK RESUME;
ALTER TASK BRONZE_TASK RESUME;

-- 验证状态
SHOW TASKS;