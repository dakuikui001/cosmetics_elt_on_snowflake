import os
import snowflake.snowpark as snowpark
from setup import SnowflakeSetupHelper 

def main():
    # 1. 检查环境变量是否存在，防止出现之前的 NoneType 错误
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    
    if not all([account, user, password]):
        print("❌ 错误: 环境变量 SNOWFLAKE_ACCOUNT, USER 或 PASSWORD 未设置")
        exit(1)

    # 连接配置
    connection_parameters = {
        "account": account,
        "user": user,
        "password": password,
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH"
        # 建议这里先不写 database/schema，让 helper 内部去切换，
        # 这样可以避免因为连接时库不存在而导致报错
    }

    try:
        session = snowpark.Session.builder.configs(connection_parameters).create()
        
        # 🔴 关键点：
        # 根据你的 setup.py 逻辑: self.catalog = f"{db_name.upper()}_DB_{env.upper()}"
        # 传入 db_name="COSMETICS", env="DEV" -> 拼接出 "COSMETICS_DB_DEV"
        # 这与你的 Terraform 完美对齐
        helper = SnowflakeSetupHelper(session, env="DEV", db_name="COSMETICS")
        
        print(f"🚀 开始执行环境初始化 (目标库: COSMETICS_DB_DEV)...")
        
        # 先清理旧环境（如果存在），确保 Base Location 能够重新绑定新 Volume
        helper.cleanup()
        
        # 执行建表逻辑
        helper.setup()
        
        # 环境验证
        if helper.validate():
            print("✨ 数据库对象部署完成且验证通过！")
        else:
            print("⚠️ 验证未通过，请检查 Snowflake 控制台。")
            exit(1)
            
    except Exception as e:
        print(f"❌ 部署过程中发生错误: {str(e)}")
        # 打印详细堆栈，方便在 GitHub Actions 调试
        import traceback
        traceback.print_exc()
        exit(1)
    finally:
        if 'session' in locals():
            session.close()

if __name__ == "__main__":
    main()