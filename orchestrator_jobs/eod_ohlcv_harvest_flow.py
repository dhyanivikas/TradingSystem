from sqlalchemy import create_engine
from orchestrator_jobs import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME
from data_flows import harvest_ohlcv_data
def execute_eod_ohlcv_harvesting():
    print("Started the EOD OHLCV data harvesting flow")
    db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
    engine = create_engine(db_connection_str)
    with engine.connect() as conn:
        harvest_ohlcv_data(conn)

    print("Ended the EOD OHLCV data harvesting flow")


execute_eod_ohlcv_harvesting()