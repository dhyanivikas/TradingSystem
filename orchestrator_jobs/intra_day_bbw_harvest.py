from sqlalchemy import create_engine
from orchestrator_jobs import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME
from orchestrator_jobs.batch_execution_configs import EOD_STOCK_KEY, INTRA_DAY_STOCK_KEY, BBW_DAILY_INTERVAL, BBW_INTRADAY_INTERVAL, INTRA_DAY_BATCH_SLEEP_TIME
from data_flows import harvest_intra_day_bbw_data
def execute_intra_day_bbw_harvesting():
    print("Started the Intra Day Bollinger Bandwidth harvesting flow")
    db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
    engine = create_engine(db_connection_str)
    with engine.connect() as conn:
        harvest_intra_day_bbw_data(conn, BBW_INTRADAY_INTERVAL, INTRA_DAY_STOCK_KEY)
    print("Ended the Intra Day Bollinger Bandwidth harvesting flow")


execute_intra_day_bbw_harvesting()