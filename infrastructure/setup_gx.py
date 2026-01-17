import os
import shutil
import great_expectations as gx
import great_expectations.expectations as gxe
from snowflake.snowpark import Session
import io

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
    # 注意：外部 Stage 路径去掉 @ 符号后的前缀处理
    stage_name = "COSMETICS_DB_DEV.COSMETICS.STAGE_COSMETICS_DB_DEV"

    # --- Step 1: 物理清理与本地初始化 ---
    if os.path.exists(gx_local_root):
        shutil.rmtree(gx_local_root)
    os.makedirs(gx_local_root, exist_ok=True)
    
    try:
        # 外部 Stage 清理通常需要通过 S3 或直接用 REMOVE (REMOVE 在某些外部 Stage 上也受限，视权限而定)
        session.sql(f"REMOVE @{stage_name}/gx_configs/great_expectations").collect()
        print(f"Cleared Stage path: @{stage_name}")
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

    # --- Step 5: 修正上传逻辑 (不再使用 PUT) ---
    print("\n--- 正在同步配置到外部 Stage (S3) ---")
    count = 0
    for root, dirs, files in os.walk(gx_local_root):
        for file in files:
            local_path = os.path.join(root, file)
            rel_dir = os.path.relpath(root, gx_local_root)
            
            # 构造 S3 内部路径
            if rel_dir == ".":
                target_path = f"gx_configs/great_expectations/{file}"
            else:
                target_path = f"gx_configs/great_expectations/{rel_dir}/{file}"
            
            try:
                # 读取本地文件内容
                with open(local_path, "rb") as f:
                    file_stream = io.BytesIO(f.read())
                
                # 使用 upload_stream 绕过 PUT 命令限制
                session.file.upload_stream(
                    input_stream=file_stream,
                    stage_location=f"@{stage_name}",
                    target_file_name=target_path,
                    overwrite=True
                )
                count += 1
            except Exception as e:
                print(f"⚠️ 文件 {file} 上传失败: {str(e)}")

    print(f"🚀 成功通过 Stream 同步了 {count} 个文件到外部 Stage。")
    session.close()

if __name__ == "__main__":
    run_gx_setup()