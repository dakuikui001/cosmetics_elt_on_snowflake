import time
from datetime import datetime
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import when_matched, when_not_matched

# ==========================================
# 1. Upserter class
# ==========================================
class Upserter:
    def __init__(self, target_table_path, join_cols, update_cols, insert_cols):
        self.target_table_path = target_table_path
        self.join_cols = join_cols
        self.update_cols = update_cols  
        self.insert_cols = insert_cols  

    def upsert(self, df_batch, batch_id):
        """Keep original logic: Execute native Merge, resolve identifier conflicts"""
        
        # 1. Aggressive deduplication: Prevent Key duplication causing Merge failure
        df_source = df_batch.drop_duplicates(self.join_cols)
        
        # 2. Get target table
        target_table = df_batch.session.table(self.target_table_path)
        
        # 3. Use aliases to protect column names
        s = df_source.alias("s")
        t = target_table.alias("t")
        
        # 4. Build Join condition
        join_condition = None
        for col in self.join_cols:
            cond = (t[col.upper()] == s[col.upper()])
            join_condition = (join_condition & cond) if join_condition is not None else cond

        # 5. Build uppercase mapping
        update_map = {col.upper(): s[col.upper()] for col in self.update_cols}
        insert_map = {col.upper(): s[col.upper()] for col in self.insert_cols}

        try:
            t.merge(
                s,
                join_condition,
                [
                    when_matched().update(update_map),
                    when_not_matched().insert(insert_map)
                ]
            )
            print(f"   -> [SUCCESS] {self.target_table_path} Merge completed")
        except Exception as e:
            error_info = f"MERGE_FAILED on {self.target_table_path}: {str(e)}"
            print(f"❌ {error_info}")
            raise Exception(error_info)

# ==========================================
# 2. Gold core class
# ==========================================
class Gold():
    def __init__(self, env, session):
        self.session = session
        self.env = env.upper()
        # 🔴 Physical path alignment
        self.catalog = f"COSMETICS_DB_{self.env}"
        self.schema = "COSMETICS"
        
        self.sl_stream = f"{self.catalog}.{self.schema}.COSMETICS_SL_STREAM"
        self.fact_table = f"{self.catalog}.{self.schema}.FACT_COSMETICS_GL"
        self.dim_brand = f"{self.catalog}.{self.schema}.DIM_BRAND_GL"
        self.dim_label = f"{self.catalog}.{self.schema}.DIM_LABEL_GL"
        self.dim_attr = f"{self.catalog}.{self.schema}.DIM_ATTRIBUTE_GL"

    def _init_upserters(self):
        """Keep original logic: Initialize Upserter list"""
        self.fact_upserter = Upserter(
            self.fact_table, ["NAME"],
            ["LABEL", "BRAND", "PRICE", "RANK", "INGREDIENTS", "UPDATE_TIME"],
            ["NAME", "LABEL", "BRAND", "PRICE", "RANK", "INGREDIENTS", "UPDATE_TIME"]
        )
        self.brand_upserter = Upserter(
            self.dim_brand, ["BRAND"], ["UPDATE_TIME"], ["BRAND", "UPDATE_TIME"]
        )
        self.label_upserter = Upserter(
            self.dim_label, ["LABEL"], ["UPDATE_TIME"], ["LABEL", "UPDATE_TIME"]
        )
        self.attr_upserter = Upserter(
            self.dim_attr, ["NAME", "ATTRIBUTE"], ["UPDATE_TIME"], ["NAME", "ATTRIBUTE", "UPDATE_TIME"]
        )

    def process_incremental(self):
        print(f"🚀 [{datetime.now()}] Starting Gold incremental task... Environment: {self.catalog}")
        start_time = time.time()
        self._init_upserters()

        df_stream = self.session.table(self.sl_stream)
        
        # Quick check for data
        if len(df_stream.limit(1).collect()) == 0:
            print("💡 No new changes in Silver, ending.")
            return 0

        # Extract incremental rows
        df_changes = df_stream.filter(F.col("METADATA$ACTION") == "INSERT").cache_result()
        curr_time = F.current_timestamp()

        try:
            # 1. FACT table processing
            fact_df = df_changes.select("NAME", "LABEL", "BRAND", "PRICE", "RANK", "INGREDIENTS") \
                                .filter(F.col("NAME").is_not_null()) \
                                .with_column("UPDATE_TIME", curr_time)
            self.fact_upserter.upsert(fact_df, "fact")

            # 2. BRAND dimension
            brand_df = df_changes.select("BRAND").distinct().filter(F.col("BRAND").is_not_null()) \
                                 .with_column("UPDATE_TIME", curr_time)
            self.brand_upserter.upsert(brand_df, "brand")

            # 3. LABEL dimension
            label_df = df_changes.select("LABEL").distinct().filter(F.col("LABEL").is_not_null()) \
                                 .with_column("UPDATE_TIME", curr_time)
            self.label_upserter.upsert(label_df, "label")

            # 4. ATTRIBUTE dimension (Unpivot logic)
            attr_cols = ["COMBINATION", "DRY", "NORMAL", "OILY", "SENSITIVE"]
            unpivoted = df_changes.select("NAME", *attr_cols).unpivot("VAL", "ATTRIBUTE", attr_cols)
            
            pos_attr = unpivoted.filter(F.col("VAL") == 1).select("NAME", "ATTRIBUTE")
            all_names = df_changes.select("NAME").distinct()
            unknowns = all_names.join(pos_attr.select("NAME").distinct(), on="NAME", how="left_anti") \
                                .with_column("ATTRIBUTE", F.lit("Unknown"))
            
            attr_df = pos_attr.union_all(unknowns).filter(F.col("NAME").is_not_null()) \
                              .with_column("UPDATE_TIME", curr_time)
            self.attr_upserter.upsert(attr_df, "attr")

            duration = int(time.time() - start_time)
            print(f"✅ Gold task succeeded, duration: {duration}s")
            return duration

        except Exception as e:
            print(f"❌ Gold process interrupted: {str(e)}")
            raise e

    def consume(self):
        """Unified Handler entry point"""
        return self.process_incremental()