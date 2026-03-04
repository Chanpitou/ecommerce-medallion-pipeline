import requests
import os
from dotenv import load_dotenv
import json
from src.utils.config import EXCHANGE_RATE_API_KEY

load_dotenv()


def extract_exchange_rates(base_currency="BRL"):
    API_KEY = EXCHANGE_RATE_API_KEY["API_KEY"]
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/{base_currency}"
    response = requests.get(url)

    os.makedirs("data/raw/exchange_rate_data", exist_ok=True)
    if response.status_code == 200:
        data = response.json()
        with open("data/raw/exchange_rate_data/exchange_rates.json", "w") as f:
            json.dump(data, f, indent=4)
    else:
        print(
            f"Failed to fetch exchange rates: {response.status_code} - {response.text}"
        )



if __name__ == "__main__":

    extract_exchange_rates()
