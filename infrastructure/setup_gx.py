import os
import shutil
import great_expectations as gx
import great_expectations.expectations as gxe
from snowflake.snowpark import Session
import io

def run_gx_setup():
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
    }
    session = Session.builder.configs(connection_parameters).create()
    
    gx_local_root = "/tmp/gx_configs"
    # 注意：外部 Stage 必须使用特定的路径处理
    stage_name = "COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV"

    if os.path.exists(gx_local_root):
        shutil.rmtree(gx_local_root)
    os.makedirs(gx_local_root, exist_ok=True)
    
    # 外部 Stage 的 REMOVE 通常是支持的
    try:
        session.sql(f"REMOVE @{stage_name}/gx_configs/").collect()
        print(f"Cleared Stage path: @{stage_name}/gx_configs/")
    except:
        pass

    context = gx.get_context(context_root_dir=gx_local_root)

    # 规则定义 (保持你的逻辑不变)
    table_rules_mapping = {
        "COSMETICS_BZ": [
            gxe.ExpectTableColumnsToMatchSet(
                column_set=[
                    "LABEL", "BRAND", "NAME", "PRICE", "RANK", 
                    "INGREDIENTS", "COMBINATION", "DRY", "NORMAL", 
                    "OILY", "SENSITIVE", "LOAD_TIME", "SOURCE_FILE", 
                    "_DQ_BATCH_ID", "SOURCE_PATH"
                ],
                exact_match=True 
            ),
            gxe.ExpectColumnValuesToNotBeNull(column="NAME"),
            gxe.ExpectColumnValuesToNotMatchRegex(column="NAME", regex=r"^#.*"),
            gxe.ExpectColumnValuesToBeBetween(column="RANK", min_value=0, max_value=5),
            gxe.ExpectColumnValuesToBeInSet(column="COMBINATION", value_set=[0, 1]),
            gxe.ExpectColumnValuesToBeInSet(column="DRY", value_set=[0, 1]),
            gxe.ExpectColumnValuesToBeInSet(column="NORMAL", value_set=[0, 1]),
            gxe.ExpectColumnValuesToBeInSet(column="OILY", value_set=[0, 1]),
            gxe.ExpectColumnValuesToBeInSet(column="SENSITIVE", value_set=[0, 1])
        ]
    }

    for table_name, expectations in table_rules_mapping.items():
        suite_name = f"{table_name.lower()}_suite"
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        for exp in expectations:
            suite.add_expectation(exp)

    # --- 关键修正：使用 SQL 方式处理外部 Stage 上传 ---
    print("\n--- 正在同步配置到外部 Stage (S3) ---")
    count = 0
    for root, dirs, files in os.walk(gx_local_root):
        for file in files:
            local_path = os.path.join(root, file)
            rel_dir = os.path.relpath(root, gx_local_root)
            
            # 外部 Stage 路径构造
            sub_path = "" if rel_dir == "." else f"{rel_dir}/"
            target_stage_path = f"@{stage_name}/gx_configs/great_expectations/{sub_path}"
            
            try:
                # 🔴 既然 PUT 不行，我们使用 Snowpark 的底层接口封装
                # 或者通过临时 Internal Stage 中转再 COPY INTO（这是最稳的 Snowflake 官方推荐做法）
                # 但为了简单，我们先尝试修复方法名：
                session._conn.upload_file(
                    local_path, 
                    stage_location=target_stage_path, 
                    overwrite=True,
                    parallel=4
                )
                count += 1
            except Exception as e:
                print(f"⚠️ 文件 {file} 上传失败: {str(e)}")

    print(f"🚀 成功同步了 {count} 个文件到外部 Stage。")
    session.close()

if __name__ == "__main__":
    run_gx_setup()