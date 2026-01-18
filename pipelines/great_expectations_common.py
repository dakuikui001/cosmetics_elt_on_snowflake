import os
import sys
import json
import gc
import threading
import pandas
from datetime import datetime
import great_expectations as gx
import great_expectations.expectations as gxe
from snowflake.snowpark import functions as F
from snowflake.snowpark import Session

# ==========================================
# 1. 基础配置
# ==========================================
gx_local_root = "/tmp/gx_configs"
BASE_PATH = os.path.join(gx_local_root, "expectations/")

# 🔴 物理对齐：默认隔离表名
DEFAULT_QUARANTINE_TABLE = "DATA_QUALITY_QUARANTINE"

_SHARED_GX_CONTEXT = None
_CACHED_SUITES_JSON = {}
gx_lock = threading.RLock() 

# ==========================================
# 2. 配置预加载
# ==========================================
def preload_all_suites():
    global _CACHED_SUITES_JSON
    if not os.path.exists(BASE_PATH):
        os.makedirs(BASE_PATH, exist_ok=True)
        print(f"📁 已创建目录: {BASE_PATH}")
    
    files = [f for f in os.listdir(BASE_PATH) if f.endswith(".json")]
    for f in files:
        suite_name = f.replace(".json", "")
        try:
            with open(os.path.join(BASE_PATH, f), "r", encoding='utf-8') as file:
                suite_dict = json.load(file)
                suite_dict.pop("name", None)
                suite_dict.pop("data_context_id", None)
                _CACHED_SUITES_JSON[suite_name] = suite_dict
            print(f"✅ 预加载 Suite: {suite_name}")
        except Exception as e:
            print(f"❌ 加载失败 {f}: {e}")

def load_suite_simple(context, suite_name):
    possible_names = [suite_name, suite_name.replace("_bz_suite", "")]
    for name in possible_names:
        try:
            return context.suites.get(name=name)
        except Exception:
            if name in _CACHED_SUITES_JSON:
                suite_data = _CACHED_SUITES_JSON[name]
                new_suite = gx.ExpectationSuite(
                    name=name, 
                    expectations=suite_data.get("expectations", [])
                )
                return context.suites.add(new_suite)
    raise FileNotFoundError(f"Suite {suite_name} 未在缓存中找到。")

# ==========================================
# 3. 核心写入函数 (关键修正点)
# ==========================================
def snowflake_iceberg_insert(df, full_table_name):
    """
    保持原逻辑：列对齐与强转
    修正点：将 save_as_table 切换为 insert_into，以保护下游 Stream 不被重置
    """
    try:
        current_session = df.session 
        
        # 保持原逻辑：动态对齐目标表 Schema
        target_table = current_session.table(full_table_name)
        target_schema = target_table.schema
        
        current_df = df
        for col in df.columns:
            current_df = current_df.with_column_renamed(col, col.upper())
            
        select_exprs = []
        for field in target_schema.fields:
            col_name = field.name.upper()
            col_type = field.datatype
            if col_name in current_df.columns:
                select_exprs.append(F.col(col_name).cast(col_type).as_(col_name))
            else:
                select_exprs.append(F.lit(None).cast(col_type).as_(col_name))
        
        df_aligned = current_df.select(*select_exprs)

        # 🔴 核心修正：
        # 原逻辑 save_as_table 会重建 Iceberg 元数据导致 Stream 丢失
        # insert_into 是纯 DML 操作，能确保下游 COSMETICS_BZ_STREAM 捕获到增量
        df_aligned.write.insert_into(full_table_name)
        print(f"✅ 写入成功: {full_table_name}")
        
    except Exception as e:
        print(f"❌ Snowflake Write Error [{full_table_name}]: {e}")
        raise e

# ==========================================
# 4. 验证与分流处理 (保持原逻辑不变)
# ==========================================
def validate_and_insert_process_batch(df, batch_id, table_name):
    if df.count() == 0:
        return

    current_session = df.session
    current_db = current_session.get_current_database().replace('"', '')
    current_schema = current_session.get_current_schema().replace('"', '')
    
    full_target_path = f"{current_db}.{current_schema}.{table_name}"
    full_quarantine_path = f"{current_db}.{current_schema}.{DEFAULT_QUARANTINE_TABLE}"
    
    temp_id_col = "_DQ_BATCH_ID"
    curr_time = datetime.now()
    
    df_with_id = df.with_column(temp_id_col, F.seq8()).cache_result()
    pd_df = df_with_id.to_pandas()
    
    for col in pd_df.select_dtypes(include=['object']).columns:
        pd_df[col] = pd_df[col].astype(str).replace(['nan', 'None', 'NaN', '<NA>'], '#NAME?')
    
    business_cols = [c for c in df.columns if c != temp_id_col]

    result = None
    with gx_lock:
        try:
            context = gx.get_context(mode="ephemeral")
            ds_name = f"ds_{table_name}_{batch_id}"
            datasource = context.data_sources.add_pandas(name=ds_name)
            asset = datasource.add_dataframe_asset(name="asset")
            
            suite_key = f"{table_name.lower()}_suite"
            suite = load_suite_simple(context, suite_key)
            
            validator = context.get_validator(
                batch_request=asset.build_batch_request(options={"dataframe": pd_df}),
                expectation_suite=suite
            )
            result = validator.validate(result_format={"result_format": "COMPLETE"})
        except Exception as e:
            print(f"⚠️ GX 运行异常，降级执行全量插入: {e}")
            snowflake_iceberg_insert(df_with_id.drop(temp_id_col), full_target_path)
            return

    try:
        if result and not result.success:
            violation_map = {}
            for r in result.results:
                if not r.success:
                    col_name = r.expectation_config.kwargs.get("column")
                    if col_name:
                        unexpected_vals = r.result.get("unexpected_list", [])
                        if unexpected_vals:
                            if col_name not in violation_map: violation_map[col_name] = []
                            violation_map[col_name].extend([str(v) for v in unexpected_vals])

            if violation_map:
                combined_condition = F.lit(False)
                for col_name, bad_vals in violation_map.items():
                    unique_bad_vals = list(set(bad_vals))
                    combined_condition = combined_condition | F.col(col_name).cast("string").in_(unique_bad_vals)

                kv_pairs = []
                for col in business_cols:
                    kv_pairs.append(F.lit(col))  
                    kv_pairs.append(F.col(col).cast("string"))  

                bad_df = df_with_id.filter(combined_condition) \
                                   .with_column("VIOLATED_RULES", F.lit("GX_VALUE_VIOLATION")) \
                                   .with_column("TABLE_NAME", F.lit(table_name)) \
                                   .with_column("GX_BATCH_ID", F.lit(str(batch_id))) \
                                   .with_column("INGESTION_TIME", F.lit(curr_time)) \
                                   .with_column("RAW_DATA", F.to_variant(F.builtin("OBJECT_CONSTRUCT")(*kv_pairs))) \
                                   .select("TABLE_NAME", "GX_BATCH_ID", "VIOLATED_RULES", "RAW_DATA", "INGESTION_TIME")
                
                snowflake_iceberg_insert(bad_df, full_quarantine_path)

                good_df = df_with_id.filter(~combined_condition).drop(temp_id_col)
                if good_df.limit(1).count() > 0:
                    snowflake_iceberg_insert(good_df, full_target_path)
                return

        snowflake_iceberg_insert(df_with_id.drop(temp_id_col), full_target_path)

    except Exception as e:
        print(f"❌ 分流处理失败: {e}")
        snowflake_iceberg_insert(df_with_id.drop(temp_id_col), full_target_path)
    finally:
        gc.collect()