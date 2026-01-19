import os
import snowflake.snowpark as snowpark
from setup import SnowflakeSetupHelper 

def main():
    # 1. Check if environment variables exist to prevent previous NoneType errors
    account = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    password = os.getenv("SNOWFLAKE_PASSWORD")
    
    if not all([account, user, password]):
        print("❌ Error: Environment variables SNOWFLAKE_ACCOUNT, USER or PASSWORD not set")
        exit(1)

    # Connection configuration
    connection_parameters = {
        "account": account,
        "user": user,
        "password": password,
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH"
        # Recommend not specifying database/schema here, let helper switch internally,
        # this avoids errors when connecting if the database doesn't exist yet
    }

    try:
        session = snowpark.Session.builder.configs(connection_parameters).create()
        
        # 🔴 Key point:
        # Based on setup.py logic: self.catalog = f"{db_name.upper()}_DB_{env.upper()}"
        # Passing db_name="COSMETICS", env="DEV" -> concatenates to "COSMETICS_DB_DEV"
        # This aligns perfectly with your Terraform
        helper = SnowflakeSetupHelper(session, env="DEV", db_name="COSMETICS")
        
        print(f"🚀 Starting environment initialization (target database: COSMETICS_DB_DEV)...")
        
        # Clean up old environment first (if exists), ensure Base Location can rebind to new Volume
        helper.cleanup()
        
        # Execute table creation logic
        helper.setup()
        
        # Environment validation
        if helper.validate():
            print("✨ Database object deployment completed and validation passed!")
        else:
            print("⚠️ Validation failed, please check Snowflake console.")
            exit(1)
            
    except Exception as e:
        print(f"❌ Error occurred during deployment: {str(e)}")
        # Print detailed stack trace for debugging in GitHub Actions
        import traceback
        traceback.print_exc()
        exit(1)
    finally:
        if 'session' in locals():
            session.close()

if __name__ == "__main__":
    main()