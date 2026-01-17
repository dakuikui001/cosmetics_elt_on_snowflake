import os
import shutil
import great_expectations as gx
import great_expectations.expectations as gxe
from snowflake.snowpark import Session

def run_gx_setup():
    # 连接配置
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
    }
    session = Session.builder.configs(connection_parameters).create()
    
    # 路径定义
    gx_local_root = "/tmp/gx_configs"
    gx_stage_path = "@COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV/gx_configs/great_expectations"

    # --- Step 1: 物理清理与本地初始化 ---
    if os.path.exists(gx_local_root):
        shutil.rmtree(gx_local_root)
    os.makedirs(gx_local_root, exist_ok=True)
    
    try:
        session.sql(f"REMOVE {gx_stage_path}").collect()
        print(f"Cleared Stage: {gx_stage_path}")
    except:
        pass

    # --- Step 2: GX 初始化 ---
    context = gx.get_context(context_root_dir=gx_local_root)

    # --- Step 3: 你的 1.10.0 规则定义 ---
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

    # --- Step 4: 构建 Suite ---
    for table_name, expectations in table_rules_mapping.items():
        suite_name = f"{table_name.lower()}_suite"
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        for exp in expectations:
            suite.add_expectation(exp)

    # --- Step 5: 你的 os.walk 同步逻辑 ---
    count = 0
    for root, dirs, files in os.walk(gx_local_root):
        for file in files:
            local_path = os.path.join(root, file)
            rel_dir = os.path.relpath(root, gx_local_root)
            target_stage = gx_stage_path if rel_dir == "." else f"{gx_stage_path}/{rel_dir}"
            
            session.file.put(
                local_file_name=local_path,
                stage_location=target_stage,
                auto_compress=False,
                overwrite=True
            )
            count += 1
    
    print(f"🚀 Successfully synced {count} files to S3.")
    session.close()

if __name__ == "__main__":
    run_gx_setup()