from snowflake.snowpark import Session

def run_bronze_step(session: Session, env: str):
    """
    任务 A: Bronze 处理 (S3 -> Iceberg + GX Validation)
    对应 SQL Handler: main_pipeline.run_bronze_step
    """
    try:
        # Lazy import: Ensure files are loaded from stored procedure IMPORTS only at runtime
        from bronze import Bronze 
        
        print(f"🚀 [MAIN] Starting Bronze layer processing (environment: {env})...")
        
        # Initialize and execute
        processor = Bronze(env, session)
        processor.consume() 
        
        return f"SUCCESS: Bronze layer processed in {env} environment."
    except Exception as e:
        print(f"❌ [MAIN] Bronze layer execution exception: {str(e)}")
        # Raise exception to notify Snowflake Task of failure
        raise e

def run_silver_step(session: Session, env: str):
    """
    任务 B: Silver 处理 (Clean -> De-duplicate -> Silver Table)
    对应 SQL Handler: main_pipeline.run_silver_step
    """
    try:
        from silver import Silver
        
        print(f"🚀 [MAIN] Starting Silver layer processing (environment: {env})...")
        
        processor = Silver(env, session)
        # Use the consume() entry point defined in silver.py
        processor.consume() 
        
        return f"SUCCESS: Silver layer processed in {env} environment."
    except Exception as e:
        print(f"❌ [MAIN] Silver layer execution exception: {str(e)}")
        raise e

def run_gold_step(session: Session, env: str):
    """
    任务 C: Gold 处理 (Aggregation -> Unpivot -> Gold Table)
    对应 SQL Handler: main_pipeline.run_gold_step
    """
    try:
        from gold import Gold
        
        print(f"🚀 [MAIN] Starting Gold layer processing (environment: {env})...")
        
        processor = Gold(env, session)
        # Use the consume() entry point defined in gold.py
        processor.consume()
        
        return f"SUCCESS: Gold layer processed in {env} environment."
    except Exception as e:
        print(f"❌ [MAIN] Gold layer execution exception: {str(e)}")
        raise e