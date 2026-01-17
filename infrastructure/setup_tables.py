import os
import snowflake.snowpark as snowpark
from setup import SnowflakeSetupHelper 

def main():
    # 连接配置，确保数据库名完全匹配
    connection_parameters = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "COSMETICS_DB_DEV",
        "schema": "COSMETICS"
    }

    session = snowpark.Session.builder.configs(connection_parameters).create()
    
    try:
        # 注意：这里 env="DEV", db_name="COSMETICS" 
        # 会触发类内部拼接出 COSMETICS_DB_DEV
        helper = SnowflakeSetupHelper(session, env="DEV", db_name="COSMETICS")
        
        print("🚀 开始执行环境初始化...")
        helper.cleanup()
        helper.setup()
        
        if helper.validate():
            print("✨ 数据库对象部署完成且验证通过！")
            
    except Exception as e:
        print(f"❌ 部署过程中发生错误: {str(e)}")
        exit(1)
    finally:
        session.close()

if __name__ == "__main__":
    main()