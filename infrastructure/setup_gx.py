import os
import shutil
import great_expectations as gx
import great_expectations.expectations as gxe
from snowflake.snowpark import Session

def run_gx_setup():
    # 建立会话仅用于清理 Stage 上的旧标记（如果需要）
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE")
    }
    session = Session.builder.configs(connection_parameters).create()
    
    gx_local_root = "/tmp/gx_configs"
    
    # 1. 清理本地旧配置
    if os.path.exists(gx_local_root):
        shutil.rmtree(gx_local_root)
    os.makedirs(gx_local_root, exist_ok=True)

    # 2. 初始化 GX 并生成规则 (你的 1.10.0 逻辑)
    context = gx.get_context(context_root_dir=gx_local_root)

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
    
    print(f"✅ GX Configs generated successfully at {gx_local_root}")
    session.close()

if __name__ == "__main__":
    run_gx_setup() 