# Table Structure Documentation

This document provides a comprehensive overview of all database tables, streams, and related objects in the Cosmetics ELT Pipeline project.

## Database and Schema

- **Database**: `COSMETICS_DB_DEV` (or `COSMETICS_DB_{ENV}` for different environments, format: `{DB_NAME}_DB_{ENV}`)
- **Schema**: `COSMETICS`
- **Table Type**: All tables are **Iceberg Tables** stored in Snowflake with external volume `VOL_S3_COSMETICS_DB_DEV` (format: `VOL_S3_{DB_NAME}_DB_{ENV}`)

---

## Bronze Layer Tables

### 1. COSMETICS_BZ

**Purpose**: Raw data ingestion table with metadata preservation and data quality validation.

**Table Type**: Iceberg Table  
**Base Location**: `medallion/bronze/cosmetics_bz/`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| LABEL | STRING | Yes | Product category or label (e.g., "Moisturizer", "Cleanser") |
| BRAND | STRING | Yes | Brand name of the cosmetic product |
| NAME | STRING | Yes | Product name |
| PRICE | DOUBLE | Yes | Product price in USD |
| RANK | DOUBLE | Yes | Product ranking or rating |
| INGREDIENTS | STRING | Yes | List of ingredients in the product |
| COMBINATION | INTEGER | Yes | Flag indicating if product is suitable for combination skin (0 or 1) |
| DRY | INTEGER | Yes | Flag indicating if product is suitable for dry skin (0 or 1) |
| NORMAL | INTEGER | Yes | Flag indicating if product is suitable for normal skin (0 or 1) |
| OILY | INTEGER | Yes | Flag indicating if product is suitable for oily skin (0 or 1) |
| SENSITIVE | INTEGER | Yes | Flag indicating if product is suitable for sensitive skin (0 or 1) |
| LOAD_TIME | TIMESTAMP | No | Timestamp when the record was loaded into Bronze layer |
| SOURCE_FILE | STRING | No | Name of the source CSV file from which the record was extracted |
| SOURCE_PATH | STRING | No | Full S3 path of the source file |

**Notes**:
- This table stores raw data as-is from CSV files with minimal transformation
- Metadata columns (`LOAD_TIME`, `SOURCE_FILE`, `SOURCE_PATH`) are automatically added during ingestion
- Data quality validation is performed using Great Expectations before insertion
- Valid records are inserted into this table, invalid records are routed to `DATA_QUALITY_QUARANTINE`

---

### 2. DATA_QUALITY_QUARANTINE

**Purpose**: Isolation table for records that fail data quality validation checks.

**Table Type**: Iceberg Table  
**Base Location**: `medallion/quarantine/data_quality_quarantine/`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| TABLE_NAME | STRING | No | Name of the source table where the violation was detected |
| GX_BATCH_ID | STRING | No | Great Expectations batch identifier for the validation run |
| VIOLATED_RULES | STRING | No | Description of the data quality rules that were violated |
| RAW_DATA | VARIANT | Yes | JSON object containing the complete raw record that failed validation |
| INGESTION_TIME | TIMESTAMP | No | Timestamp when the record was quarantined |

**Notes**:
- Records are automatically routed here when they fail Great Expectations validation
- `RAW_DATA` is stored as a VARIANT type containing all original column values as key-value pairs
- This table enables data quality monitoring and allows for manual review and correction of invalid records

---

## Silver Layer Tables

### 3. COSMETICS_SL

**Purpose**: Cleansed and standardized data table with deduplication applied.

**Table Type**: Iceberg Table  
**Base Location**: `medallion/silver/cosmetics_sl/`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| LABEL | STRING | Yes | Product category or label (cleansed) |
| BRAND | STRING | Yes | Brand name (cleansed) |
| NAME | STRING | No | Product name (primary key for deduplication) |
| PRICE | DOUBLE | Yes | Product price in USD |
| RANK | DOUBLE | Yes | Product ranking or rating |
| INGREDIENTS | STRING | Yes | List of ingredients (normalized: "no info", "#name?", "visit*" → "Unknown") |
| COMBINATION | INTEGER | Yes | Flag for combination skin suitability (0 or 1) |
| DRY | INTEGER | Yes | Flag for dry skin suitability (0 or 1) |
| NORMAL | INTEGER | Yes | Flag for normal skin suitability (0 or 1) |
| OILY | INTEGER | Yes | Flag for oily skin suitability (0 or 1) |
| SENSITIVE | INTEGER | Yes | Flag for sensitive skin suitability (0 or 1) |
| CLEANSED_TIME | TIMESTAMP | No | Timestamp when the record was last updated in Silver layer |

**Notes**:
- Data is incrementally processed from `COSMETICS_BZ_STREAM` (only new/changed records)
- Null values are replaced with 'Unknown' for strings and 0 for numeric fields
- Ingredient placeholders are normalized: "no info", "#name?", "visit*" are converted to "Unknown"
- Deduplication is performed on `NAME` column (latest record based on `LOAD_TIME` wins)
- Metadata columns (`LOAD_TIME`, `SOURCE_FILE`, `SOURCE_PATH`) are removed in this layer

---

## Gold Layer Tables

### 4. FACT_COSMETICS_GL

**Purpose**: Fact table containing product-level metrics for analytics and reporting.

**Table Type**: Iceberg Table  
**Base Location**: `medallion/gold/fact_cosmetics_gl/`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| NAME | STRING | No | Product name (primary key) |
| LABEL | STRING | Yes | Product category/label (foreign key to DIM_LABEL_GL) |
| BRAND | STRING | Yes | Brand name (foreign key to DIM_BRAND_GL) |
| PRICE | DOUBLE | Yes | Product price in USD |
| RANK | DOUBLE | Yes | Product ranking or rating |
| INGREDIENTS | STRING | Yes | List of ingredients |
| UPDATE_TIME | TIMESTAMP | No | Timestamp when the record was last updated in Gold layer |

**Notes**:
- This is the main fact table for dimensional modeling
- Skin type flags (COMBINATION, DRY, NORMAL, OILY, SENSITIVE) are moved to `DIM_ATTRIBUTE_GL` via unpivot transformation
- Optimized for analytical queries and Power BI reporting
- Uses merge (upsert) logic: updates existing records by `NAME`, inserts new ones

---

### 5. DIM_BRAND_GL

**Purpose**: Dimension table for brand reference data.

**Table Type**: Iceberg Table  
**Base Location**: `medallion/gold/dim_brand_gl/`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| BRAND | STRING | No | Brand name (primary key) |
| UPDATE_TIME | TIMESTAMP | No | Timestamp when the brand record was last updated |

**Notes**:
- Contains unique brand names extracted from the fact table
- Used for filtering and grouping in analytical queries
- Automatically populated from `COSMETICS_SL_STREAM` during Gold layer processing

---

### 6. DIM_LABEL_GL

**Purpose**: Dimension table for product category/label reference data.

**Table Type**: Iceberg Table  
**Base Location**: `medallion/gold/dim_label_gl/`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| LABEL | STRING | No | Product category/label (primary key) |
| UPDATE_TIME | TIMESTAMP | No | Timestamp when the label record was last updated |

**Notes**:
- Contains unique product categories/labels extracted from the fact table
- Used for filtering and grouping in analytical queries
- Automatically populated from `COSMETICS_SL_STREAM` during Gold layer processing

---

### 7. DIM_ATTRIBUTE_GL

**Purpose**: Dimension table for skin type attributes, created via unpivot transformation.

**Table Type**: Iceberg Table  
**Base Location**: `medallion/gold/dim_attribute_gl/`

| Column Name | Data Type | Nullable | Description |
|------------|-----------|----------|-------------|
| NAME | STRING | No | Product name (part of composite primary key) |
| ATTRIBUTE | STRING | No | Skin type attribute name (part of composite primary key). Values: "COMBINATION", "DRY", "NORMAL", "OILY", "SENSITIVE", or "Unknown" |
| UPDATE_TIME | TIMESTAMP | No | Timestamp when the attribute record was last updated |

**Notes**:
- Created by unpivoting skin type flags (COMBINATION, DRY, NORMAL, OILY, SENSITIVE) from Silver layer
- Each product can have multiple rows (one per applicable skin type)
- Products with all flags set to 0 get a single row with ATTRIBUTE = "Unknown"
- Composite primary key: (NAME, ATTRIBUTE)
- Enables flexible querying of products by skin type attributes

---

## Streams

### 8. COSMETICS_BZ_STREAM

**Purpose**: Change Data Capture (CDC) stream on `COSMETICS_BZ` table for incremental processing.

**Type**: Stream  
**Source Table**: `COSMETICS_BZ`

**Metadata Columns** (automatically added by Snowflake):
- `METADATA$ACTION`: Action type ("INSERT", "DELETE", "UPDATE")
- `METADATA$ISUPDATE`: Boolean indicating if the change is an update
- `METADATA$ROW_ID`: Unique identifier for the row

**Notes**:
- Tracks all changes (inserts) to the Bronze table
- Consumed by Silver layer for incremental processing
- Stream offset advances automatically when consumed

---

### 9. COSMETICS_SL_STREAM

**Purpose**: Change Data Capture (CDC) stream on `COSMETICS_SL` table for incremental processing.

**Type**: Stream  
**Source Table**: `COSMETICS_SL`

**Metadata Columns** (automatically added by Snowflake):
- `METADATA$ACTION`: Action type ("INSERT", "DELETE", "UPDATE")
- `METADATA$ISUPDATE`: Boolean indicating if the change is an update
- `METADATA$ROW_ID`: Unique identifier for the row

**Notes**:
- Tracks all changes (inserts/updates) to the Silver table
- Consumed by Gold layer for incremental processing
- Stream offset advances automatically when consumed

---

### 10. STREAM_TRIGGER_COSMETICS_DB_DEV

**Purpose**: Stream on S3 stage directory to detect new file arrivals for pipeline triggering.

**Type**: Stream on Stage  
**Source Stage**: `STAGE_TRIGGER_COSMETICS_DB_DEV` (points to `s3://bucket-for-snowflake-projects/cosmetics_etl_project/raw/`)

**Notes**:
- Monitors the `raw/` directory in S3 for new CSV files
- Used by `BRONZE_TASK` with `SYSTEM$STREAM_HAS_DATA('COSMETICS_DB_DEV.COSMETICS.STREAM_TRIGGER_COSMETICS_DB_DEV')` function to trigger pipeline execution
- Enables event-driven orchestration of the Bronze layer processing
- Stream name format: `STREAM_TRIGGER_{DB_NAME}_DB_{ENV}` (matches infrastructure naming convention)

---

## Data Flow Summary

```
S3 (raw/) 
  ↓
COSMETICS_BZ (Bronze - Raw with metadata)
  ↓ [COSMETICS_BZ_STREAM]
COSMETICS_SL (Silver - Cleansed & Deduplicated)
  ↓ [COSMETICS_SL_STREAM]
FACT_COSMETICS_GL (Gold - Fact table)
  ├─→ DIM_BRAND_GL (Gold - Brand dimension)
  ├─→ DIM_LABEL_GL (Gold - Label dimension)
  └─→ DIM_ATTRIBUTE_GL (Gold - Attribute dimension)

Invalid Data → DATA_QUALITY_QUARANTINE
```

---

## Key Design Principles

1. **Medallion Architecture**: Clear separation into Bronze (raw), Silver (cleansed), and Gold (curated) layers
2. **Incremental Processing**: Streams enable CDC-based incremental updates, avoiding full table scans
3. **ACID Compliance**: Iceberg tables provide transactional consistency and time travel capabilities
4. **Data Quality**: Great Expectations validation at Bronze layer with automatic quarantine routing
5. **Dimensional Modeling**: Gold layer follows star schema pattern with fact and dimension tables
6. **Metadata Tracking**: Load timestamps and source file information preserved through Bronze layer
7. **Type Safety**: Strong schema enforcement via Snowpark DataFrames and Iceberg table definitions

---

## Table Relationships

- **FACT_COSMETICS_GL.BRAND** → **DIM_BRAND_GL.BRAND** (many-to-one)
- **FACT_COSMETICS_GL.LABEL** → **DIM_LABEL_GL.LABEL** (many-to-one)
- **FACT_COSMETICS_GL.NAME** → **DIM_ATTRIBUTE_GL.NAME** (one-to-many, as products can have multiple skin type attributes)

---

## Maintenance Notes

- All tables use Iceberg format for ACID transactions and time travel
- Streams must be consumed to advance offsets; unused streams can cause storage growth
- Quarantine table should be periodically reviewed and cleaned
- Gold layer dimension tables are automatically maintained via merge operations
- Table locations in S3 are managed through the external volume `VOL_S3_COSMETICS_DB_DEV` (format: `VOL_S3_{DB_NAME}_DB_{ENV}`)
