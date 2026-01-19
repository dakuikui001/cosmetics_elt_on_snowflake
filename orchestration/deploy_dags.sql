/********************************************************************************
  File: orchestration/deploy_dags.sql
  Purpose: Register stored procedures and orchestrate Task DAG based on physical infrastructure
********************************************************************************/

-- =====================================================
-- 1. Environment context setup
-- =====================================================
USE ROLE ACCOUNTADMIN;
USE DATABASE COSMETICS_DB_DEV;
USE SCHEMA COSMETICS;

-- =====================================================
-- 2. Register stored procedures (Procedures)
-- =====================================================

-- Register Bronze procedure (data cleansing and quality validation)
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

-- Register Silver procedure (deduplication and table merge)
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

-- Register Gold procedure (metric processing and Fact table merge)
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
-- 3. Define scheduled tasks (Tasks) - strictly aligned with setup_infra.sql
-- =====================================================

-- Root task: Monitor S3 directory Stage change Stream
CREATE OR REPLACE TASK BRONZE_TASK
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 MINUTE'
    -- 🔴 Fix: Align with STREAM_TRIGGER_COSMETICS_DB_DEV in setup_infra.sql
    WHEN SYSTEM$STREAM_HAS_DATA('COSMETICS_DB_DEV.COSMETICS.STREAM_TRIGGER_COSMETICS_DB_DEV')
AS
    CALL RUN_BRONZE_PROC('DEV');

-- Silver task: Triggers after Bronze succeeds and when Bronze table has changes
CREATE OR REPLACE TASK SILVER_TASK
    WAREHOUSE = 'COMPUTE_WH'
    AFTER BRONZE_TASK
    -- 🔴 Ensure this Stream has been created in setup_tables.py
    WHEN SYSTEM$STREAM_HAS_DATA('COSMETICS_DB_DEV.COSMETICS.COSMETICS_BZ_STREAM')
AS
    CALL RUN_SILVER_PROC('DEV');

-- Gold task: Triggers after Silver succeeds and when Silver table has changes
CREATE OR REPLACE TASK GOLD_TASK
    WAREHOUSE = 'COMPUTE_WH'
    AFTER SILVER_TASK
    -- 🔴 Ensure this Stream has been created in setup_tables.py
    WHEN SYSTEM$STREAM_HAS_DATA('COSMETICS_DB_DEV.COSMETICS.COSMETICS_SL_STREAM')
AS
    CALL RUN_GOLD_PROC('DEV');

-- =====================================================
-- 4. Activate task flow (must enable from leaf to root)
-- =====================================================
-- Suspend before activation to ensure updates are applied
ALTER TASK IF EXISTS GOLD_TASK SUSPEND;
ALTER TASK IF EXISTS SILVER_TASK SUSPEND;
ALTER TASK IF EXISTS BRONZE_TASK SUSPEND;

-- Start task chain
ALTER TASK GOLD_TASK RESUME;
ALTER TASK SILVER_TASK RESUME;
ALTER TASK BRONZE_TASK RESUME;

-- Verify status
SHOW TASKS;