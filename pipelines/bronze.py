from snowflake.snowpark import functions as F
import great_expectations_common as gec
import time
import re
import os

class Bronze():
    def __init__(self, env, session):
        self.session = session
        env_upper = env.upper()
        
        # 1. Physical database name alignment (COSMETICS_DB_DEV)
        self.env_db = f"COSMETICS_DB_{env_upper}"
        
        # 2. Physical Stage name alignment (STAGE_COSMETICS_DB_DEV)
        # Note: This must match the name in setup_infra.sql
        self.stage_name = f"@{self.env_db}.COSMETICS.STAGE_{self.env_db}"
        
        self.landing_path = "raw"
        
        # 3. 物理 Stream 名对齐 (STREAM_TRIGGER_COSMETICS_DB_DEV)
        # 🔴 这是解决“循环触发”和“不消费”的关键
        self.stage_stream = f"{self.env_db}.COSMETICS.STREAM_TRIGGER_COSMETICS_DB_DEV"
        
    def _get_new_files(self, table_name, pattern):
        """Keep original logic: Get files not yet loaded via LIST"""
        files_on_stage = self.session.sql(f"LIST {self.stage_name}/{self.landing_path}").collect()
        
        all_files = [f['name'].split('/')[-1] for f in files_on_stage 
                     if f['name'].split('/')[-1].startswith(pattern) and f['name'].endswith('.csv')]
        
        try:
            # Check already loaded files to prevent duplicate loading
            processed_df = self.session.table(f"{self.env_db}.COSMETICS.{table_name}").select("SOURCE_FILE").distinct()
            processed_files = {row.SOURCE_FILE for row in processed_df.to_local_iterator()}
        except Exception:
            processed_files = set()
            
        new_files = [f for f in all_files if f not in processed_files]
        return new_files

    def _force_consume_stream(self):
        """
        Keep original logic: Force consume Stream offset.
        Use WHERE 1=0 to trigger Snowflake Stream pointer movement.
        """
        print(f"🔄 Force consuming Stream ({self.stage_stream})...")
        
        consume_sql = f"""
            INSERT INTO {self.env_db}.COSMETICS.COSMETICS_BZ (SOURCE_FILE)
            SELECT 'dummy_ignore' 
            FROM {self.stage_stream}
            WHERE 1=0
        """
        
        try:
            self.session.sql(consume_sql).collect()
            print("✅ Stream pointer moved successfully, state has been reset.")
        except Exception as e:
            print(f"⚠️ Force consume Stream failed: {str(e)}")

    def _read_and_process_incremental(self, schema_str, file_pattern, table_name):
        """Keep original logic: Core processing logic"""
        print(f"\n--- Starting to process table: {table_name} ---")
        
        new_files = self._get_new_files(table_name, file_pattern)
        if not new_files:
            print(f"☕ {table_name}: No new files detected, cleaning up Stream...")
            self._force_consume_stream()
            return

        print(f"📂 Matched {len(new_files)} new files: {new_files}")
        regex_pattern = f".*({'|'.join([re.escape(f) for f in new_files])}).*"

        try:
            col_definitions = [c.strip().split(' ') for c in schema_str.split(',')]
            column_projections = ", ".join([
                f"CAST(${i+1} AS {parts[1]}) AS {parts[0].upper()}" 
                for i, parts in enumerate(col_definitions)
            ])

            sql_query = f"""
                SELECT 
                    {column_projections},
                    METADATA$FILENAME AS SOURCE_PATH,
                    SPLIT_PART(METADATA$FILENAME, '/', -1) AS SOURCE_FILE,
                    CURRENT_TIMESTAMP() AS LOAD_TIME
                FROM {self.stage_name}/{self.landing_path}
                (
                  FILE_FORMAT => '{self.env_db}.COSMETICS.BZ_CSV_FORMAT', 
                  PATTERN => '{regex_pattern}'
                )
            """

            df = self.session.sql(sql_query)
            
            # Keep original logic: Call GX validation
            batch_id = int(time.time())
            gec.validate_and_insert_process_batch(df=df, batch_id=batch_id, table_name=table_name)
            
            # Clear Stream after processing to prevent Task loop
            self._force_consume_stream()
            print(f"🚀 {table_name}: Incremental load and validation completed successfully.")

        except Exception as e:
            import traceback
            print(f"❌ {table_name} processing exception, keeping Stream offset unchanged for retry:")
            print(traceback.format_exc())
            
    def consume_cosmetics_bz(self):
        schema = "Label STRING, Brand STRING, Name STRING, Price DOUBLE, Rank DOUBLE, Ingredients STRING, Combination INT, Dry INT, Normal INT, Oily INT, Sensitive INT"
        self._read_and_process_incremental(schema, "cosmetics", "COSMETICS_BZ")

    def consume(self):
        """Keep original logic: Sync GX rules and execute"""
        start = int(time.time())
        print(f"\n--- Starting Bronze Layer Processing ---")
        
        local_dir = "/tmp/gx_configs/expectations"
        os.makedirs(local_dir, exist_ok=True)
        
        # 🔴 Dynamically get Stage path (align with latest physical environment)
        stage_name_full = f"{self.env_db}.COSMETICS.STAGE_{self.env_db}"
        relative_path = "gx_configs/great_expectations/expectations"
        
        print(f"📥 Syncing validation rules from S3 Stage (@{stage_name_full})...")
        
        try:
            files_df = self.session.sql(f"LIST @{stage_name_full}/{relative_path}").collect()
            for file_info in files_df:
                full_s3_path = file_info['name'] 
                if not full_s3_path.endswith('.json'): continue
                
                pure_file_name = full_s3_path.split('/')[-1]
                snowflake_path = f"@{stage_name_full}/{relative_path}/{pure_file_name}"
                
                input_stream = self.session.file.get_stream(snowflake_path)
                with open(os.path.join(local_dir, pure_file_name), "wb") as f:
                    f.write(input_stream.read())
            
            gec.BASE_PATH = local_dir
            gec.preload_all_suites()
            print(f"✅ Validation rules loaded successfully.")
            
        except Exception as e:
            print(f"⚠️ Sync rules warning (S3 may be empty): {str(e)}")

        self.consume_cosmetics_bz()
        print(f"--- Completed Bronze Layer: {int(time.time()) - start} seconds ---")