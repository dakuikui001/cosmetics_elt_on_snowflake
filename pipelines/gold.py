import time
from datetime import datetime
from snowflake.snowpark import functions as F
from snowflake.snowpark.functions import when_matched, when_not_matched

# ==========================================
# 1. 基础 Upserter 类 (最终方案：原生 API 版)
# ==========================================
class Upserter:
    def __init__(self, target_table_path, join_cols, update_cols, insert_cols):
        self.target_table_path = target_table_path
        self.join_cols = join_cols
        self.update_cols = update_cols  
        self.insert_cols = insert_cols  

    def upsert(self, df_batch, batch_id):
        """保持原逻辑：执行原生 Merge，解决标识符冲突"""
        
        # 1. 强力去重：防止 Key 重复导致 Merge 失败
        df_source = df_batch.drop_duplicates(self.join_cols)
        
        # 2. 获取目标表
        target_table = df_batch.session.table(self.target_table_path)
        
        # 3. 使用别名保护列名
        s = df_source.alias("s")
        t = target_table.alias("t")
        
        # 4. 构造 Join 条件
        join_condition = None
        for col in self.join_cols:
            cond = (t[col.upper()] == s[col.upper()])
            join_condition = (join_condition & cond) if join_condition is not None else cond

        # 5. 构造大写映射
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
            print(f"   -> [SUCCESS] {self.target_table_path} Merge 完成")
        except Exception as e:
            error_info = f"MERGE_FAILED on {self.target_table_path}: {str(e)}"
            print(f"❌ {error_info}")
            raise Exception(error_info)

# ==========================================
# 2. Gold 层核心类
# ==========================================
class Gold():
    def __init__(self, env, session):
        self.session = session
        self.env = env.upper()
        # 🔴 物理路径对齐
        self.catalog = f"COSMETICS_DB_{self.env}"
        self.schema = "COSMETICS"
        
        self.sl_stream = f"{self.catalog}.{self.schema}.COSMETICS_SL_STREAM"
        self.fact_table = f"{self.catalog}.{self.schema}.FACT_COSMETICS_GL"
        self.dim_brand = f"{self.catalog}.{self.schema}.DIM_BRAND_GL"
        self.dim_label = f"{self.catalog}.{self.schema}.DIM_LABEL_GL"
        self.dim_attr = f"{self.catalog}.{self.schema}.DIM_ATTRIBUTE_GL"

    def _init_upserters(self):
        """保持原逻辑：初始化 Upserter 列表"""
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
        print(f"🚀 [{datetime.now()}] 启动 Gold 增量任务... 环境: {self.catalog}")
        start_time = time.time()
        self._init_upserters()

        df_stream = self.session.table(self.sl_stream)
        
        # 快速检查是否有数据
        if len(df_stream.limit(1).collect()) == 0:
            print("💡 Silver 无新变更，结束。")
            return 0

        # 提取增量行
        df_changes = df_stream.filter(F.col("METADATA$ACTION") == "INSERT").cache_result()
        curr_time = F.current_timestamp()

        try:
            # 1. FACT 表加工
            fact_df = df_changes.select("NAME", "LABEL", "BRAND", "PRICE", "RANK", "INGREDIENTS") \
                                .filter(F.col("NAME").is_not_null()) \
                                .with_column("UPDATE_TIME", curr_time)
            self.fact_upserter.upsert(fact_df, "fact")

            # 2. BRAND 维度
            brand_df = df_changes.select("BRAND").distinct().filter(F.col("BRAND").is_not_null()) \
                                 .with_column("UPDATE_TIME", curr_time)
            self.brand_upserter.upsert(brand_df, "brand")

            # 3. LABEL 维度
            label_df = df_changes.select("LABEL").distinct().filter(F.col("LABEL").is_not_null()) \
                                 .with_column("UPDATE_TIME", curr_time)
            self.label_upserter.upsert(label_df, "label")

            # 4. ATTRIBUTE 维度 (Unpivot 逻辑)
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
            print(f"✅ Gold 任务成功，耗时: {duration}s")
            return duration

        except Exception as e:
            print(f"❌ Gold 流程中断: {str(e)}")
            raise e

    def consume(self):
        """统一 Handler 入口"""
        return self.process_incremental()