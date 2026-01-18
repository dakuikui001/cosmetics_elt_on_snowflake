import time
from snowflake.snowpark import Session

class SnowflakeSetupHelper():
    def __init__(self, session: Session, env: str, db_name: str):
        self.session = session
        self.env = env.upper()
        self.catalog = f"{db_name.upper()}_DB_{self.env}"
        self.db_name = db_name.upper()      
        
        # 引用基础设施资源 (需与 setup_infra.sql 中的名称对应)
        self.external_volume = f'VOL_S3_{self.catalog}'
        self.stage_name = f'STAGE_{self.catalog}' 
        self.initialized = False

    def create_db(self):
        """[Step 1] 环境上下文切换"""
        print(f"--- [Step 1] Setting Context for {self.catalog}.{self.db_name} ---")
        self.session.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}").collect()
        self.session.use_database(self.catalog)
        self.session.sql(f"CREATE SCHEMA IF NOT EXISTS {self.db_name}").collect()
        self.session.use_schema(self.db_name)
        print(f"✓ Current Context: {self.session.get_current_database()}.{self.session.get_current_schema()}")

    def _create_iceberg_table(self, table_name, columns_sql, location):
        """
        内部方法：创建 Iceberg 表 (回归原始逻辑)
        """
        if not location.endswith('/'):
            location += '/'
            
        print(f"Creating Iceberg table {table_name} at {location}...", end='')
        
        # 还原：直接创建，不加 ALTER，不加多余参数
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
        """
        内部方法：创建表级 Stream (还原原始逻辑)
        Snowflake 在创建 Stream 时会自动尝试为底层表开启 Change Tracking
        """
        print(f"Creating Stream {stream_name} on {table_name}...", end='')
        self.session.sql(f"""
            CREATE OR REPLACE STREAM {self.catalog}.{self.db_name}.{stream_name}
            ON TABLE {self.catalog}.{self.db_name}.{table_name}
        """).collect()
        print("Done")

    def setup(self):
        """部署 Medallion 架构所有逻辑对象 - 还原你之前成功的顺序"""
        start = int(time.time())
        print(f"\n🚀 Starting Snowflake Logical Setup for: {self.catalog}")
        
        self.create_db()

        # 1. Bronze 层
        self._create_iceberg_table(
            "COSMETICS_BZ", 
            "LABEL STRING, BRAND STRING, NAME STRING, PRICE DOUBLE, RANK DOUBLE, INGREDIENTS STRING, COMBINATION INTEGER, DRY INTEGER, NORMAL INTEGER, OILY INTEGER, SENSITIVE INTEGER, LOAD_TIME TIMESTAMP, SOURCE_FILE STRING", 
            "medallion/bronze/cosmetics_bz/"
        )
        self._create_stream("COSMETICS_BZ_STREAM", "COSMETICS_BZ")

        # 2. Silver 层
        self._create_iceberg_table(
            "COSMETICS_SL", 
            "LABEL STRING, BRAND STRING, NAME STRING, PRICE DOUBLE, RANK DOUBLE, INGREDIENTS STRING, COMBINATION INTEGER, DRY INTEGER, NORMAL INTEGER, OILY INTEGER, SENSITIVE INTEGER, CLEANSED_TIME TIMESTAMP", 
            "medallion/silver/cosmetics_sl/"
        )
        self._create_stream("COSMETICS_SL_STREAM", "COSMETICS_SL")

        # 3. Gold 层
        gold_tables = {
            "FACT_COSMETICS_GL": "NAME STRING, LABEL STRING, BRAND STRING, PRICE DOUBLE, RANK DOUBLE, INGREDIENTS STRING, UPDATE_TIME TIMESTAMP",
            "DIM_BRAND_GL": "BRAND STRING, UPDATE_TIME TIMESTAMP",
            "DIM_LABEL_GL": "LABEL STRING, UPDATE_TIME TIMESTAMP",
            "DIM_ATTRIBUTE_GL": "NAME STRING, ATTRIBUTE STRING, UPDATE_TIME TIMESTAMP"
        }
        for name, ddl in gold_tables.items():
            self._create_iceberg_table(name, ddl, f"medallion/gold/{name.lower()}/")

        # 4. Data Quality 隔离表
        self._create_iceberg_table(
            "DATA_QUALITY_QUARANTINE",
            "TABLE_NAME STRING, GX_BATCH_ID STRING, VIOLATED_RULES STRING, RAW_DATA STRING, INGESTION_TIME TIMESTAMP",
            "medallion/quarantine/data_quality_quarantine/"
        )

        print(f"✅ Setup completed in {int(time.time()) - start} seconds")

    def cleanup(self):
        """物理级彻底清理"""
        print(f"\n--- Starting Full Physical Cleanup ---")
        full_path = f"{self.catalog}.{self.db_name}"
        
        tables = [
            "COSMETICS_BZ", "COSMETICS_SL", "FACT_COSMETICS_GL", 
            "DIM_BRAND_GL", "DIM_LABEL_GL", "DIM_ATTRIBUTE_GL", 
            "DATA_QUALITY_QUARANTINE"
        ]
        for t in tables:
            print(f"Dropping table {t}... ", end='')
            self.session.sql(f"DROP TABLE IF EXISTS {full_path}.{t} CASCADE").collect()
            print("Done")

        print(f"Attempting S3 physical cleanup via REMOVE...", end='')
        try:
            full_stage_path = f"@{full_path}.{self.stage_name}"
            self.session.sql(f"REMOVE {full_stage_path}/medallion/").collect()
            print("Done")
        except Exception as e:
            print(f"Notice: S3 path cleanup handled by Snowflake. {e}")

    def validate(self):
        """环境验证"""
        print(f"\n--- [Step 3] Validating Environment for {self.catalog}.{self.db_name} ---")
        
        expected_tables = [
            "COSMETICS_BZ", "COSMETICS_SL", 
            "FACT_COSMETICS_GL", "DIM_BRAND_GL", "DIM_LABEL_GL", "DIM_ATTRIBUTE_GL",
            "DATA_QUALITY_QUARANTINE"
        ]
        expected_streams = ["COSMETICS_BZ_STREAM", "COSMETICS_SL_STREAM"]
        
        missing_objects = []
        try:
            self.session.use_database(self.catalog)
            self.session.use_schema(self.db_name)
            
            existing_tables = [row['name'] for row in self.session.sql(f"SHOW TABLES IN SCHEMA {self.db_name}").collect()]
            for table in expected_tables:
                if table.upper() not in [t.upper() for t in existing_tables]:
                    missing_objects.append(table)

            existing_streams = [row['name'] for row in self.session.sql(f"SHOW STREAMS IN SCHEMA {self.db_name}").collect()]
            for stream in expected_streams:
                if stream.upper() not in [s.upper() for s in existing_streams]:
                    missing_objects.append(stream)

            return len(missing_objects) == 0
        except Exception:
            return False