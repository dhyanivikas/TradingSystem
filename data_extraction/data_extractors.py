import requests
import os

from data_extraction import TWELVE_API_BASE_URL, RAPID_API_HOST, EOD_STOCK_KEY1, EOD_STOCK_KEY2


def get_realtime_stock_price(stock_symbol, rapid_api_key):
    base_url = TWELVE_API_BASE_URL
    rapid_api_host = RAPID_API_HOST
    url = f"{base_url}/price"
    output_size = "30"

    querystring = {
        "format": "json",
        "outputsize": output_size,
        "symbol": stock_symbol
    }

    headers = {
        "x-rapidapi-key": rapid_api_key,
        "x-rapidapi-host": rapid_api_host
    }

    response = requests.get(url, headers=headers, params=querystring)
    price_data = response.json()
    price = float(price_data['price'])
    return price


def fetch_adx_data(stock_symbol, interval, output_size, rapid_api_key):
    base_url = TWELVE_API_BASE_URL
    rapid_api_host = RAPID_API_HOST
    url = f"{base_url}/adx"
    querystring = {"symbol":{stock_symbol},"interval":{interval},"format":"json","outputsize":{output_size},"time_period":"14"}
    headers = {
        "x-rapidapi-key": rapid_api_key,
        "x-rapidapi-host": rapid_api_host
    }
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()



def fetch_ohlcv_data(stock_symbol, interval, output_size, rapid_api_key):
    base_url = TWELVE_API_BASE_URL
    rapid_api_host = RAPID_API_HOST
    url = f"{base_url}/time_series"

    querystring = {"symbol":{stock_symbol},"interval":{interval},"format":"json","outputsize":{output_size}}
    headers = {
        "x-rapidapi-key": rapid_api_key,
        "x-rapidapi-host": rapid_api_host
    }
    response = requests.get(url, headers=headers, params=querystring)
    return response.json()


def fetch_bbw_data(stock_symbol, interval, standard_deviation, time_period, output_size, rapid_api_key):
    base_url = TWELVE_API_BASE_URL
    rapid_api_host = RAPID_API_HOST
    url = f"{base_url}/bbands"
    print("parameters for data extraction:-> ", stock_symbol, interval, standard_deviation, time_period, output_size)

    querystring = {"symbol": {stock_symbol}, "interval": {interval}, "sd": {standard_deviation}, "series_type": "close",
                   "ma_type": "SMA", "time_period": {time_period}, "outputsize": {output_size}, "format": "json"}

    headers = {
        "x-rapidapi-key": rapid_api_key,
        "x-rapidapi-host": rapid_api_host
    }

    response = requests.get(url, headers=headers, params=querystring)

    return response.json()