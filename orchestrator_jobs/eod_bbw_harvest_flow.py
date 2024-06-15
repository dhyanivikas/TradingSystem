from sqlalchemy import create_engine
from orchestrator_jobs import DB_USER, DB_PASSWORD, DB_HOST, DB_NAME
from data_flows import harvest_bbw_data
def execute_eod_bbw_harvesting():
    print("Started the EOD Bollinger Bandwidth harvesting flow")
    db_connection_str = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}'
    engine = create_engine(db_connection_str)
    with engine.connect() as conn:
        harvest_bbw_data(conn)
    #harvest_bbw_data(engine)
    print("Ended the EOD Bollinger Bandwidth harvesting flow")


execute_eod_bbw_harvesting()