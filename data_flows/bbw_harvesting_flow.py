# DAILY BBW Data

import pandas as pd
import time
from data_extraction import fetch_bbw_data
from data_flows.bbw_harvesting_flow_configs import BBW_TIME_PERIOD, BBW_OUTPUT_SIZE, BBW_STANDARD_DEVIATION, FREE_STOCK_SLEEP_TIME, INTRA_DAY_BATCH_SLEEP_TIME
from data_transformation import get_bbands_dataframe
from sqlalchemy import text
from utils import playSoundBite
from sqlalchemy import create_engine

def harvest_bbw_data(engine, interval, api_key):

    # Execute the SQL query to get the data from the table.
    query = "SELECT id, stock_symbol FROM stocks_watch_list"
    df = pd.read_sql(query, engine)

    for stock_symbol in df['stock_symbol']:

        query = text("SELECT max(trading_date) FROM stocks_daily_bbw_values WHERE stock_symbol = :stock_symbol")
        result = engine.execute(query, {'stock_symbol':stock_symbol})

        max_date = result.scalar()
        print(f"Maximum trading date for {stock_symbol}: {max_date}")
        technical_data = fetch_bbw_data(stock_symbol, interval, BBW_STANDARD_DEVIATION, BBW_TIME_PERIOD, BBW_OUTPUT_SIZE, api_key)
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


def harvest_intra_day_bbw_data(engine, interval, api_key):

    # Execute the SQL query to get the data from the table.
    query = "SELECT id, stock_symbol FROM stocks_watch_list where track_intraday=1"
    df = pd.read_sql(query, engine)
    while(True):
        for stock_symbol in df['stock_symbol']:
            technical_data = fetch_bbw_data(stock_symbol, interval, BBW_STANDARD_DEVIATION, BBW_TIME_PERIOD, BBW_OUTPUT_SIZE, api_key)
            df = get_bbands_dataframe(technical_data)
            df['trading_date'] = pd.to_datetime(df['trading_date'])
            df = df.sort_values(by='trading_date', ascending=False).reset_index(drop=True)
            latest_bbw = df['bbw'].iloc[0]
            latest_trading_date = df['trading_date'].iloc[0]
            latest_bbw_percentile = df['bbw_percentile'].iloc[0]
            latest_bbw_upper = df['bb_upper'].iloc[0]
            latest_bbw_lower = df['bb_lower'].iloc[0]

            print(f"Stock: {stock_symbol}")
            print(f"Latest BBW: {latest_bbw}, Trading Time: {latest_trading_date}")
            print(f"Stock {stock_symbol} Bollinger Bandwidth and is in {latest_bbw_percentile:.2f} %ile of last {BBW_OUTPUT_SIZE} samples")
            textToPlay = f"Stock {stock_symbol} has squeezed in Bollinger Bandwidth and is in {latest_bbw_percentile:.2f} percentile of last {BBW_OUTPUT_SIZE} samples"
            if (latest_bbw_percentile < 10):
                playSoundBite(textToPlay)

            time.sleep(FREE_STOCK_SLEEP_TIME)  # 8 seconds delay to avoid rate limit exceed
        time.sleep(INTRA_DAY_BATCH_SLEEP_TIME)
