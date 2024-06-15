import pandas as pd
from scipy.stats import percentileofscore

def get_bbands_dataframe(json_data):
    # Extract the relevant information from the JSON
    stock_symbol = json_data['meta']['symbol']
    values = json_data['values']

    # Create a DataFrame from the values
    df = pd.DataFrame(values)

    # Convert 'datetime' and relevant columns to the appropriate data types
    df['trading_date'] = pd.to_datetime(df['datetime'])
    df['bb_upper'] = df['upper_band'].astype(float)
    df['bb_mid'] = df['middle_band'].astype(float)
    df['bb_lower'] = df['lower_band'].astype(float)

    # Add the stock symbol column
    df['stock_symbol'] = stock_symbol

    # Calculate the Bollinger Bandwidth (bbw)
    df['bbw'] = (df['bb_upper'] - df['bb_lower']) / df['bb_mid']

    # Calculate the percentile for the Bollinger Bandwidth
    df['bbw_percentile'] = df['bbw'].rank(pct=True) * 100

    # Select the required columns
    df = df[['trading_date', 'bb_upper', 'bb_mid', 'bb_lower', 'bbw', 'stock_symbol', 'bbw_percentile']]

    return df


def get_adx_dataframe(json_data):
    # Extract the relevant information from the JSON
    stock_symbol = json_data['meta']['symbol']
    values = json_data['values']

    # Create a DataFrame from the values
    df = pd.DataFrame(values)

    # Convert 'datetime' and 'adx' columns to the appropriate data types
    df['trading_date'] = pd.to_datetime(df['datetime'])
    df['adx'] = df['adx'].astype(float)

    # Add the stock symbol column
    df['stock_symbol'] = stock_symbol
    df['adx_percentile'] = df['adx'].rank(pct=True) * 100

    # Select the required columns
    df = df[['trading_date', 'adx', 'stock_symbol', 'adx_percentile']]

    return df


def get_ohlcv_dataframe(json_data):
    # Extract the relevant information from the JSON
    stock_symbol = json_data['meta']['symbol']
    values = json_data['values']

    # Create a DataFrame from the values
    df = pd.DataFrame(values)

    # Convert 'datetime' and relevant columns to the appropriate data types
    df['trading_date'] = pd.to_datetime(df['datetime'])
    df['open'] = df['open'].astype(float)
    df['high'] = df['high'].astype(float)
    df['low'] = df['low'].astype(float)
    df['close'] = df['close'].astype(float)
    df['volume'] = df['volume'].astype(int)

    # Add the stock symbol column
    df['stock_symbol'] = stock_symbol

    # Calculate the percentile for the close prices
    df['close_price_percentile'] = df['close'].rank(pct=True) * 100

    # Select the required columns
    df = df[['trading_date', 'open', 'high', 'low', 'close', 'volume', 'stock_symbol', 'close_price_percentile']]

    return df
