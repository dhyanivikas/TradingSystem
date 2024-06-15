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
