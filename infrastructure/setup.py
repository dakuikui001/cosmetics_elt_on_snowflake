import time
from snowflake.snowpark import Session

class SnowflakeSetupHelper():
    def __init__(self, session: Session, env: str, db_name: str):
        self.session = session
        # 🔴 修正：完全匹配 Terraform 和 setup_infra.sql 定义的数据库名 (COSMETICS_DB_DEV)
        self.catalog = f"{db_name.upper()}_DB_{env.upper()}"
        self.db_name = db_name.upper()      
        
        # 🔴 修正：引用 setup_infra.sql 中重新命名的物理资源名 (VOL_S3_COSMETICS_DB_DEV)
        self.external_volume = f'VOL_S3_{self.catalog}'
        self.initialized = False

    def create_db(self):
        """[Step 1] 确保上下文环境正确"""
        print(f"--- [Step 1] Setting Context for {self.catalog}.{self.db_name} ---")
        # 基础 Database 和 Schema 由 Terraform 确保，这里仅做切换和补漏
        self.session.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}").collect()
        self.session.use_database(self.catalog)
        self.session.sql(f"CREATE SCHEMA IF NOT EXISTS {self.db_name}").collect()
        self.session.use_schema(self.db_name)
        print(f"✓ Current Context: {self.session.get_current_database()}.{self.session.get_current_schema()}")

    def _create_iceberg_table(self, table_name, columns_sql, location):
        """内部方法：创建 Iceberg 表 (受管模式)"""
        if not location.endswith('/'):
            location += '/'
            
        print(f"Creating Iceberg table {table_name} at {location}...", end='')
        # 使用 OR REPLACE 确保基础设施更新能即时生效
        self.session.sql(f"""
            CREATE OR REPLACE ICEBERG TABLE {self.catalog}.{self.db_name}.{table_name} (
                {columns_sql}
            )
            CATALOG = 'SNOWFLAKE'
            EXTERNAL_VOLUME = '{self.external_volume}'
            BASE_LOCATION = '{location}'
            COMMENT = 'Managed Iceberg Table in Medallion Architecture'
        """).collect()
        print("Done")

    def _create_stream(self, stream_name, table_name):
        """内部方法：创建表级 Stream (用于 Medallion 链路触发)"""
        print(f"Creating Stream {stream_name} on {table_name}...", end='')
        self.session.sql(f"""
            CREATE OR REPLACE STREAM {self.catalog}.{self.db_name}.{stream_name}
            ON TABLE {self.catalog}.{self.db_name}.{table_name}
            SHOW_INITIAL_ROWS = TRUE
        """).collect()
        print("Done")

    def setup(self):
        """部署 Medallion 架构所有逻辑表和流对象"""
        start = int(time.time())
        print(f"\n🚀 Starting Snowflake Logical Setup for: {self.catalog}")
        
        self.create_db()

        # 1. Bronze 层：原始数据快照
        self._create_iceberg_table(
            "COSMETICS_BZ", 
            "LABEL STRING, BRAND STRING, NAME STRING, PRICE DOUBLE, RANK DOUBLE, INGREDIENTS STRING, COMBINATION INTEGER, DRY INTEGER, NORMAL INTEGER, OILY INTEGER, SENSITIVE INTEGER, LOAD_TIME TIMESTAMP, SOURCE_FILE STRING", 
            "medallion/bronze/"
        )
        self._create_stream("COSMETICS_BZ_STREAM", "COSMETICS_BZ")

        # 2. Silver 层：清洗过滤层
        self._create_iceberg_table(
            "COSMETICS_SL", 
            "LABEL STRING, BRAND STRING, NAME STRING, PRICE DOUBLE, RANK DOUBLE, INGREDIENTS STRING, COMBINATION INTEGER, DRY INTEGER, NORMAL INTEGER, OILY INTEGER, SENSITIVE INTEGER, CLEANSED_TIME TIMESTAMP", 
            "medallion/silver/"
        )
        self._create_stream("COSMETICS_SL_STREAM", "COSMETICS_SL")

        # 3. Gold 层：分析指标表
        gold_tables = {
            "FACT_COSMETICS_GL": "NAME STRING, LABEL STRING, BRAND STRING, PRICE DOUBLE, RANK DOUBLE, INGREDIENTS STRING, UPDATE_TIME TIMESTAMP",
            "DIM_BRAND_GL": "BRAND STRING, UPDATE_TIME TIMESTAMP",
            "DIM_LABEL_GL": "LABEL STRING, UPDATE_TIME TIMESTAMP",
            "DIM_ATTRIBUTE_GL": "NAME STRING, ATTRIBUTE STRING, UPDATE_TIME TIMESTAMP"
        }
        for name, ddl in gold_tables.items():
            self._create_iceberg_table(name, ddl, f"medallion/gold/{name.lower()}/")

        # 4. Data Quality：异常数据隔离表 (真正的 Iceberg 格式)
        self._create_iceberg_table(
            "DATA_QUALITY_QUARANTINE",
            "TABLE_NAME STRING, GX_BATCH_ID STRING, VIOLATED_RULES STRING, RAW_DATA STRING, INGESTION_TIME TIMESTAMP",
            "medallion/quarantine/"
        )

        print(f"✅ Setup completed in {int(time.time()) - start} seconds")

    def cleanup(self):
        """物理级清理：删除逻辑表，但不触碰 setup_infra.sql 创建的占位表"""
        print(f"\n--- Starting Logical Cleanup ---")
        full_path = f"{self.catalog}.{self.db_name}"
        
        # 1. 删除逻辑 Stream
        streams = ["COSMETICS_BZ_STREAM", "COSMETICS_SL_STREAM"]
        for s in streams:
            print(f"Dropping stream {s}... ", end='')
            self.session.sql(f"DROP STREAM IF EXISTS {full_path}.{s}").collect()
            print("Done")
        
        # 2. 删除所有正式的 Iceberg 表
        # 注意：这里不包含 STG_PIPE_PLACEHOLDER，以保证 Pipe 通道不被完全破坏
        tables = [
            "COSMETICS_BZ", "COSMETICS_SL", "FACT_COSMETICS_GL", 
            "DIM_BRAND_GL", "DIM_LABEL_GL", "DIM_ATTRIBUTE_GL", 
            "DATA_QUALITY_QUARANTINE"
        ]
        for t in tables:
            print(f"Dropping Iceberg table {t}... ", end='')
            self.session.sql(f"DROP TABLE IF EXISTS {full_path}.{t}").collect()
            print("Done")
        
        print("✓ Cleanup finished.")

    def validate(self):
        """环境验证"""
        print(f"\n--- [Step 3] Validating Environment ---")
        try:
            self.session.use_database(self.catalog)
            # 检查关键对象数量
            res = self.session.sql(f"SHOW TABLES IN SCHEMA {self.db_name}").collect()
            stream_res = self.session.sql(f"SHOW STREAMS IN SCHEMA {self.db_name}").collect()
            
            print(f"✓ Found {len(res)} total tables (including placeholder) and {len(stream_res)} streams.")
            return True
        except Exception as e:
            print(f"✕ Validation Failed: {e}")
            return False