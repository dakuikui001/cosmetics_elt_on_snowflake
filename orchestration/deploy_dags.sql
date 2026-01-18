-- =====================================================
-- 1. 环境上下文设置
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE COSMETICS_DB_DEV;
USE SCHEMA COSMETICS;

-- =====================================================
-- 2. 注册存储过程 (Procedures)
-- =====================================================

-- 注册 Bronze 过程
CREATE OR REPLACE PROCEDURE RUN_BRONZE_PROC(env STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas', 'great-expectations==1.10.0')
IMPORTS = (
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/bronze.py', 
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/great_expectations_common.py',
    '@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/python_code/main_pipeline.py'
)
HANDLER = 'main_pipeline.run_bronze_step';

-- 注册 Silver 过程
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

-- 注册 Gold 过程
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
-- 3. 定义调度任务 (Tasks) - 串联 DAG
-- =====================================================

-- 根任务：监控 S3 文件到达产生的 Stream
CREATE OR REPLACE TASK BRONZE_TASK
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('COSMETICS_DB_DEV.COSMETICS.TRIGGER_S3_FILE_STREAM')
AS
    CALL RUN_BRONZE_PROC('DEV');

-- Silver 任务：紧随 Bronze 成功后
CREATE OR REPLACE TASK SILVER_TASK
    WAREHOUSE = 'COMPUTE_WH'
    AFTER BRONZE_TASK
AS
    CALL RUN_SILVER_PROC('DEV');

-- Gold 任务：紧随 Silver 成功后
CREATE OR REPLACE TASK GOLD_TASK
    WAREHOUSE = 'COMPUTE_WH'
    AFTER SILVER_TASK
AS
    CALL RUN_GOLD_PROC('DEV');

-- =====================================================
-- 4. 激活任务流 (必须从叶子到根开启)
-- =====================================================
ALTER TASK GOLD_TASK RESUME;
ALTER TASK SILVER_TASK RESUME;
ALTER TASK BRONZE_TASK RESUME;

SHOW TASKS;