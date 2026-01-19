/********************************************************************************
  File: infrastructure/setup_infra.sql
  Purpose: One-click creation of all physical channels (Integration, Volume, Stage, Pipe)
  Updates:
    - Changed Integration and Volume to IF NOT EXISTS to prevent AWS identity ID reset
    - Stage and Pipe remain OR REPLACE to support logical updates
********************************************************************************/

-- 1. Environment initialization (prerequisite: Terraform has created DB and SCHEMA)
USE DATABASE COSMETICS_DB_DEV;
USE SCHEMA COSMETICS; 

-- 2. Create storage integration (IAM credentials for S3 communication)
-- 🔴 Key fix: Use IF NOT EXISTS.
-- This creates the object only if it doesn't exist. If it already exists, it preserves the original STORAGE_AWS_EXTERNAL_ID,
-- avoiding the need to update trust relationships in AWS console every time.
CREATE STORAGE INTEGRATION IF NOT EXISTS INT_S3_COSMETICS_DB_DEV
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::040591921557:role/snowflake_access_role_new'
  STORAGE_ALLOWED_LOCATIONS = ('s3://bucket-for-snowflake-projects/cosmetics_etl_project/');

-- 3. Create Iceberg external volume (physical location for Iceberg table writes)
-- 🔴 Key fix: Use IF NOT EXISTS.
-- Prevents regeneration of External ID, ensuring Iceberg tables always have permission to write to S3.
CREATE EXTERNAL VOLUME IF NOT EXISTS VOL_S3_COSMETICS_DB_DEV
   STORAGE_LOCATIONS = (
      (
         NAME = 'loc_cosmetics_dev'
         STORAGE_PROVIDER = 'S3'
         STORAGE_BASE_URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/'
         STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::040591921557:role/snowflake_access_role_new-volume'
      )
   );

-- 4. Create external Stage (for COPY INTO data loading)
-- Stage can remain OR REPLACE because it references the Integration name and doesn't change underlying permission identity.
CREATE OR REPLACE STAGE STAGE_COSMETICS_DB_DEV
  URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/'
  STORAGE_INTEGRATION = INT_S3_COSMETICS_DB_DEV;

-- 5. Create trigger Stage with directory functionality (for monitoring raw/ directory)
CREATE OR REPLACE STAGE STAGE_TRIGGER_COSMETICS_DB_DEV
  URL = 's3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/'
  STORAGE_INTEGRATION = INT_S3_COSMETICS_DB_DEV
  DIRECTORY = (ENABLE = TRUE, AUTO_REFRESH = TRUE);

-- 6. Create directory table Stream (monitor file arrivals)
CREATE OR REPLACE STREAM STREAM_TRIGGER_COSMETICS_DB_DEV
  ON STAGE STAGE_TRIGGER_COSMETICS_DB_DEV;

-- 7. Create file format
CREATE OR REPLACE FILE FORMAT BZ_CSV_FORMAT
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
    NULL_IF = ('NULL', '');

-- 8. Create standalone placeholder table and Pipe
-- Ensure Pipe target table always exists
CREATE TABLE IF NOT EXISTS COSMETICS_DB_DEV.COSMETICS.STG_PIPE_PLACEHOLDER (
    raw_content VARIANT,
    ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create or replace Pipe, targeting placeholder table  
CREATE OR REPLACE PIPE GET_ARN_PIPE
  AUTO_INGEST = TRUE
  AS
  COPY INTO COSMETICS_DB_DEV.COSMETICS.STG_PIPE_PLACEHOLDER
  FROM @STAGE_TRIGGER_COSMETICS_DB_DEV;

-- Deployment confirmation
SELECT 'SUCCESS' as STATUS, 'Infrastructure deployed. Integration/Volume preserved if existed.' as MESSAGE;