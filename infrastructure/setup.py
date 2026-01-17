import time
from snowflake.snowpark import Session

class SnowflakeSetupHelper():
    def __init__(self, session: Session, env: str, db_name: str):
        self.session = session
        # 匹配数据库名习惯：COSMETICS_DB_DEV
        self.catalog = f"{db_name.upper()}_DB_{env.upper()}"
        self.db_name = db_name.upper()      
        
        # 预定义资源 (必须与 Terraform 定义的 Resource Name 一致)
        self.external_volume = 'COSMETICS_S3_VOLUME'
        self.stage_name = 'COSMETICS_S3_STAGE'
        self.initialized = False

    def create_db(self):
        """[Step 1] 确保上下文环境正确"""
        print(f"--- [Step 1] Ensuring Database & Schema ---")
        # 数据库和 Schema 由 Terraform 创建，这里做 IF NOT EXISTS 保险处理
        self.session.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}").collect()
        self.session.sql(f"USE DATABASE {self.catalog}").collect()
        self.session.sql(f"CREATE SCHEMA IF NOT EXISTS {self.db_name}").collect()
        self.session.sql(f"USE SCHEMA {self.db_name}").collect()
        print(f"✓ Context set to {self.catalog}.{self.db_name}")

    def _create_iceberg_table(self, table_name, columns_sql, location):
        """内部方法：创建 Iceberg 表"""
        print(f"Creating Iceberg table {table_name}...", end='')
        self.session.sql(f"""
            CREATE ICEBERG TABLE IF NOT EXISTS {self.catalog}.{self.db_name}.{table_name} (
                {columns_sql}
            )
            CATALOG = 'SNOWFLAKE'
            EXTERNAL_VOLUME = '{self.external_volume}'
            BASE_LOCATION = '{location}'
        """).collect()
        print("Done")

    def _create_stream(self, stream_name, table_name):
        """内部方法：创建表级 Stream"""
        print(f"Creating Stream {stream_name} on {table_name}...", end='')
        self.session.sql(f"""
            CREATE STREAM IF NOT EXISTS {self.catalog}.{self.db_name}.{stream_name}
            ON TABLE {self.catalog}.{self.db_name}.{table_name}
            SHOW_INITIAL_ROWS = TRUE
        """).collect()
        print("Done")

    def setup(self):
        """一键安装所有表和【表级】Stream"""
        start = int(time.time())
        print(f"\nStarting Snowflake Setup for: {self.catalog}")
        
        self.create_db()

        # 1. Bronze 层：表 + 驱动 Silver 的 Stream
        self._create_iceberg_table(
            "COSMETICS_BZ", 
            "Label STRING, Brand STRING, Name STRING, Price DOUBLE, Rank DOUBLE, Ingredients STRING, Combination INTEGER, Dry INTEGER, Normal INTEGER, Oily INTEGER, Sensitive INTEGER, load_time TIMESTAMP, source_file STRING", 
            "medallion/bronze/cosmetics_bz/"
        )
        self._create_stream("COSMETICS_BZ_STREAM", "COSMETICS_BZ")

        # 2. Silver 层：表 + 驱动 Gold 的 Stream (补全此处)
        self._create_iceberg_table(
            "COSMETICS_SL", 
            "LABEL STRING, BRAND STRING, NAME STRING, PRICE DOUBLE, RANK DOUBLE, INGREDIENTS STRING, COMBINATION INTEGER, DRY INTEGER, NORMAL INTEGER, OILY INTEGER, SENSITIVE INTEGER, UPDATE_TIME TIMESTAMP", 
            "medallion/silver/cosmetics_sl/"
        )
        self._create_stream("COSMETICS_SL_STREAM", "COSMETICS_SL")

        # 3. Gold 层 (分析维度表)
        gold_tables = {
            "FACT_COSMETICS_GL": "NAME STRING, LABEL STRING, BRAND STRING, PRICE DOUBLE, RANK DOUBLE, INGREDIENTS STRING, UPDATE_TIME TIMESTAMP",
            "DIM_BRAND_GL": "BRAND STRING, UPDATE_TIME TIMESTAMP",
            "DIM_LABEL_GL": "LABEL STRING, UPDATE_TIME TIMESTAMP",
            "DIM_ATTRIBUTE_GL": "NAME STRING, ATTRIBUTE STRING, UPDATE_TIME TIMESTAMP"
        }
        for name, ddl in gold_tables.items():
            self._create_iceberg_table(name, ddl, f"medallion/gold/{name.lower()}/")

        # 4. 数据质量隔离表
        self._create_iceberg_table(
            "DATA_QUALITY_QUARANTINE",
            "table_name STRING, gx_batch_id STRING, violated_rules STRING, raw_data STRING, ingestion_time TIMESTAMP",
            "gx_configs/data_quality_quarantine/"
        )

        print(f"✅ Setup completed in {int(time.time()) - start} seconds")

    def cleanup(self):
        """物理级彻底清理：删除表和表级 Stream"""
        print(f"\n--- Starting Full Physical Cleanup ---")
        full_path = f"{self.catalog}.{self.db_name}"
        
        # 1. 删除表级 Stream (注意：不要删除 TRIGGER_S3_FILE_STREAM，那是 Terraform 管的)
        streams = ["COSMETICS_BZ_STREAM", "COSMETICS_SL_STREAM"]
        for s in streams:
            print(f"Dropping stream {s}...", end='')
            self.session.sql(f"DROP STREAM IF EXISTS {full_path}.{s}").collect()
            print("Done")
        
        # 2. 删除所有 Iceberg 表
        tables = ["COSMETICS_BZ", "COSMETICS_SL", "FACT_COSMETICS_GL", "DIM_BRAND_GL", "DIM_LABEL_GL", "DIM_ATTRIBUTE_GL", "DATA_QUALITY_QUARANTINE"]
        for t in tables:
            print(f"Dropping table {t}...", end='')
            self.session.sql(f"DROP TABLE IF EXISTS {full_path}.{t}").collect()
            print("Done")
        
        print("✓ Cleanup finished.")

    def validate(self):
        """环境验证"""
        print(f"\n--- [Step 3] Validating Environment ---")
        try:
            self.session.sql(f"USE DATABASE {self.catalog}").collect()
            # 检查关键表和 Stream 是否存在
            res = self.session.sql(f"SHOW TABLES IN SCHEMA {self.db_name}").collect()
            stream_res = self.session.sql(f"SHOW STREAMS IN SCHEMA {self.db_name}").collect()
            
            print(f"✓ Found {len(res)} tables and {len(stream_res)} streams.")
            return True
        except Exception as e:
            print(f"✕ Validation Failed: {e}")
            return False