import os

EOD_STOCK_KEY = os.getenv('EOD_STOCK_KEY1')
INTRA_DAY_STOCK_KEY = os.getenv('EOD_STOCK_KEY2')

BBW_DAILY_INTERVAL = '1day'
BBW_INTRADAY_INTERVAL = '30min'
BBW_STANDARD_DEVIATION = '2'
BBW_TIME_PERIOD = '20'
BBW_OUTPUT_SIZE = '1000'
INTRA_DAY_BATCH_SLEEP_TIME = 1801