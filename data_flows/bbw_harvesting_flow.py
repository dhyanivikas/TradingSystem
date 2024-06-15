# DAILY BBW Data

import pandas as pd
import time
from data_extraction import fetch_bbw_data
from data_flows.bbw_harvesting_flow_configs import BBW_DAILY_INTERVAL, BBW_TIME_PERIOD, BBW_OUTPUT_SIZE, BBW_STANDARD_DEVIATION, FREE_STOCK_SLEEP_TIME, EOD_STOCK_KEY1
from data_transformation import get_bbands_dataframe
from sqlalchemy import text
from sqlalchemy import create_engine

def harvest_bbw_data(engine):

    # Execute the SQL query to get the data from the table.
    query = "SELECT id, stock_symbol FROM stocks_watch_list"
    df = pd.read_sql(query, engine)

    for stock_symbol in df['stock_symbol']:

        query = text("SELECT max(trading_date) FROM stocks_daily_bbw_values WHERE stock_symbol = :stock_symbol")
        result = engine.execute(query, {'stock_symbol':stock_symbol})

        max_date = result.scalar()
        print(f"Maximum trading date for {stock_symbol}: {max_date}")
        technical_data = fetch_bbw_data(stock_symbol, BBW_DAILY_INTERVAL, BBW_STANDARD_DEVIATION, BBW_TIME_PERIOD, BBW_OUTPUT_SIZE, EOD_STOCK_KEY1)
        df = get_bbands_dataframe(technical_data)
        df['trading_date'] = pd.to_datetime(df['trading_date'])

        if max_date is None:  # It means the stock was never in the table. persis the dataframe
            print("inserting whole data for: ", stock_symbol)
            df.to_sql(name='stocks_daily_bbw_values', con=engine, if_exists='append', index=False)
        else:
            print("inserting delta for: ", stock_symbol)
            max_date = pd.to_datetime(max_date)
            df = df[df['trading_date'] > max_date]
            df.to_sql(name='stocks_daily_bbw_values', con=engine, if_exists='append', index=False)
        time.sleep(FREE_STOCK_SLEEP_TIME)  # 8 seconds delay to avoid rate limit exceed

