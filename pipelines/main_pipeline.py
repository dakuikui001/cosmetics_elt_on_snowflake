from snowflake.snowpark import Session

def run_bronze_step(session: Session, env: str):
    """
    任务 A: Bronze 处理 (S3 -> Iceberg + GX Validation)
    对应 SQL Handler: main_pipeline.run_bronze_step
    """
    try:
        # 延迟导入：确保在运行时才从存储过程的 IMPORTS 中加载文件
        from bronze import Bronze 
        
        print(f"🚀 [MAIN] 开始执行 Bronze 层加工 (环境: {env})...")
        
        # 初始化并执行
        processor = Bronze(env, session)
        processor.consume() 
        
        return f"SUCCESS: Bronze layer processed in {env} environment."
    except Exception as e:
        print(f"❌ [MAIN] Bronze 层执行异常: {str(e)}")
        # 抛出异常以通知 Snowflake Task 任务失败
        raise e

def run_silver_step(session: Session, env: str):
    """
    任务 B: Silver 处理 (Clean -> De-duplicate -> Silver Table)
    对应 SQL Handler: main_pipeline.run_silver_step
    """
    try:
        from silver import Silver
        
        print(f"🚀 [MAIN] 开始执行 Silver 层加工 (环境: {env})...")
        
        processor = Silver(env, session)
        # 统一使用我们在 silver.py 中定义的 consume() 入口
        processor.consume() 
        
        return f"SUCCESS: Silver layer processed in {env} environment."
    except Exception as e:
        print(f"❌ [MAIN] Silver 层执行异常: {str(e)}")
        raise e

def run_gold_step(session: Session, env: str):
    """
    任务 C: Gold 处理 (Aggregation -> Unpivot -> Gold Table)
    对应 SQL Handler: main_pipeline.run_gold_step
    """
    try:
        from gold import Gold
        
        print(f"🚀 [MAIN] 开始执行 Gold 层加工 (环境: {env})...")
        
        processor = Gold(env, session)
        # 统一使用我们在 gold.py 中定义的 consume() 入口
        processor.consume()
        
        return f"SUCCESS: Gold layer processed in {env} environment."
    except Exception as e:
        print(f"❌ [MAIN] Gold 层执行异常: {str(e)}")
        raise e