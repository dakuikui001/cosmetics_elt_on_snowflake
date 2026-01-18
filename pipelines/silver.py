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
        
        # 1. 微批次内去重 (根据业务主键和加载时间取最新)
        window_spec = Window.partition_by(self.join_col).order_by(F.col("LOAD_TIME").desc())
        df_final = df_incremental.with_column("rn", F.row_number().over(window_spec)) \
                                 .filter(F.col("rn") == 1) \
                                 .drop("rn")

        affected_rows = df_final.count() 

        # 2. 获取目标表对象
        target_table = session.table(self.target_table_name)

        # 3. 构造映射 (确保列名大写以匹配 Snowflake 习惯)
        mapping = {col.upper(): df_final[col.upper()] for col in self.biz_columns}
        mapping["UPDATE_TIME"] = F.current_timestamp()

        # 4. 执行 Merge
        if affected_rows > 0:
            print(f"🚀 正在合并 {affected_rows} 条数据至 {self.target_table_name}...")
            target_table.merge(
                df_final,
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
        # 🔴 修正：对齐新的数据库命名格式 COSMETICS_DB_DEV
        self.catalog = f"COSMETICS_DB_{env_upper}"
        self.schema = "COSMETICS"
    
    def preprocessing(self, df):
        """基础清洗"""
        return df.fillna('Unknown').fillna(0)

    def _run_process(self, stream_name, upserter_obj, transform_func):
        # 🔴 修正：Stream 名字需要补全数据库前缀，确保存储过程能跨 Schema 识别
        full_stream_name = f"{self.catalog}.{self.schema}.{stream_name}"
        print(f"📡 扫描增量 Stream: {full_stream_name}...")
        
        df_stream = self.session.table(full_stream_name)
        
        # 只处理 INSERT 动作的数据
        df_new = df_stream.filter(F.col("METADATA$ACTION") == "INSERT")
        
        if len(df_new.limit(1).collect()) == 0:
            print("☕ 无增量数据。")
            return 0

        df_transformed = transform_func(df_new)
        
        # 只保留业务列和 LOAD_TIME，彻底隔离元数据列对 Merge 的干扰
        cols_to_keep = upserter_obj.biz_columns + ["LOAD_TIME"]
        df_final_input = df_transformed.select(*cols_to_keep)

        return upserter_obj.upsert(df_final_input)

    def upsert_cosmetics_sl(self):
        """业务方法：化妆品表合并逻辑"""
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
            stream_name="COSMETICS_BZ_STREAM", # 这里的名字会被 _run_process 补全
            upserter_obj=upserter,
            transform_func=transform
        )

    def consume(self):
        """统一调度入口"""
        start = int(time.time())
        print(f"\n[Silver Layer Pipeline Started] 环境: {self.catalog}")
        
        count = self.upsert_cosmetics_sl()
        
        print(f"✅ 处理完成。条数: {count}，耗时: {int(time.time()) - start}s")