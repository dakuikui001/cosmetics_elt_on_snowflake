from snowflake.snowpark.window import Window
from snowflake.snowpark import functions as F
import time

class SnowparkUpserter:
    def __init__(self, target_table_name, join_col, biz_columns):
        self.target_table_name = target_table_name
        self.join_col = join_col
        self.biz_columns = biz_columns

    def upsert(self, df_incremental):
        session = df_incremental.session
        
        # 1. 设置别名 "src"
        df_incremental = df_incremental.alias("src")
        
        # 🔴 修正：不要使用 "src.NAME" 字符串引用，改用 df_incremental 对象引用
        # 这样 Snowpark 会生成 src."NAME"，从而避免标识符编译错误
        window_spec = Window.partition_by(df_incremental[self.join_col]) \
                            .order_by(df_incremental["LOAD_TIME"].desc())
        
        df_final = df_incremental.with_column("rn", F.row_number().over(window_spec)) \
                                 .filter(F.col("rn") == 1) \
                                 .drop("rn")

        affected_rows = df_final.count() 

        # 2. 获取目标表对象
        target_table = session.table(self.target_table_name)

        # 3. 构造映射 
        # 修正：将 "UPDATE_TIME" 改为 "CLEANSED_TIME"
        mapping = {col.upper(): df_final[col.upper()] for col in self.biz_columns}
        mapping["CLEANSED_TIME"] = F.current_timestamp()

        # 4. 执行 Merge
        if affected_rows > 0:
            print(f"🚀 正在合并 {affected_rows} 条数据至 {self.target_table_name}...")
            target_table.merge(
                df_final,
                # 🔴 修正：此处也使用对象引用
                target_table[self.join_col] == df_final[self.join_col],
                [
                    F.when_matched().update(mapping),
                    F.when_not_matched().insert(mapping)
                ]
            )
        return affected_rows
    
class Silver:
    def __init__(self, env, session):
        self.session = session
        env_upper = env.upper()
        self.catalog = f"COSMETICS_DB_{env_upper}"
        self.schema = "COSMETICS"
    
    def preprocessing(self, df):
        return df.fillna('Unknown').fillna(0)

    def _run_process(self, stream_name, upserter_obj, transform_func):
        full_stream_name = f"{self.catalog}.{self.schema}.{stream_name}"
        print(f"📡 扫描增量 Stream: {full_stream_name}...")
        
        df_stream = self.session.table(full_stream_name)
        df_new = df_stream.filter(F.col("METADATA$ACTION") == "INSERT")
        
        if len(df_new.limit(1).collect()) == 0:
            print("☕ 无增量数据。")
            return 0

        df_transformed = transform_func(df_new)
        
        # 隔离元数据列，只保留业务列和 LOAD_TIME
        cols_to_keep = upserter_obj.biz_columns + ["LOAD_TIME"]
        df_final_input = df_transformed.select(*cols_to_keep)

        return upserter_obj.upsert(df_final_input)

    def upsert_cosmetics_sl(self):
        target_name = f"{self.catalog}.{self.schema}.COSMETICS_SL"
        biz_cols = ["LABEL", "BRAND", "NAME", "PRICE", "RANK", "INGREDIENTS", 
                    "COMBINATION", "DRY", "NORMAL", "OILY", "SENSITIVE"]

        upserter = SnowparkUpserter(target_name, "NAME", biz_cols)

        def transform(df):
            df = self.preprocessing(df)
            return df.with_column("INGREDIENTS", 
                F.when(
                    (F.lower(F.col("INGREDIENTS")) == "no info") | 
                    (F.lower(F.col("INGREDIENTS")) == "#name?") | 
                    (F.lower(F.col("INGREDIENTS")).startswith("visit")),
                    F.lit("Unknown")
                ).otherwise(F.col("INGREDIENTS"))
            )

        return self._run_process(
            stream_name="COSMETICS_BZ_STREAM", 
            upserter_obj=upserter,
            transform_func=transform
        )

    def consume(self):
        start = int(time.time())
        print(f"\n[Silver Layer Pipeline Started] 环境: {self.catalog}")
        count = self.upsert_cosmetics_sl()
        print(f"✅ 处理完成。条数: {count}，耗时: {int(time.time()) - start}s")