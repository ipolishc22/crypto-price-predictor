import os
import time
import datetime
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("CRYPTOCOMPARE_API_KEY")
BASE_URL = "https://min-api.cryptocompare.com/data/v2/histoday"


def fetch_chunk(to_ts):
    '''Fetch up to 2000 days of OHCLV ending at to_ts timestamp'''

    params = {
        "fsym": "BTC",
        "tsym": "USD",
        "limit": 2000,
        "toTs": to_ts,
        "api_key": API_KEY
    }

    response = requests.get(BASE_URL, params=params)
    data = response.json()

    if data['Response'] != "Success":
        raise Exception(f"API error: {data.get('Message', 'Unknown error')}")
    
    return data['Data']['Data']

def parse_candles(candles):
    '''Clean the data and put it a pandas dataframe'''
    df = pd.DataFrame(candles)
    df = df[['time', 'open', 'high', 'low', 'close', 'volumeto']]
    df.rename(columns={'time':'timestamp', 'volumeto':'volume'}, inplace=True)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit="s")
    df =  df[df['close'] > 0]
    df.set_index('timestamp', inplace=True)
    return df

def fetch_all_historical():
    '''Fetch all historic Bitcoin data starting in 2010'''
    print("Fetching all historical data")
    all_candles = []
    to_ts = int(datetime.datetime.now().timestamp())
    earliest_ts = int(datetime.datetime(2010, 7, 1).timestamp())

    while to_ts > earliest_ts:
        print(f"  Fetching chunk ending {datetime.datetime.fromtimestamp(to_ts).date()}...")
        candles = fetch_chunk(to_ts)


        if not candles:
            break

        all_candles = candles + all_candles
        to_ts = candles[0]['time'] - 1
        time.sleep(0.5)

    df = parse_candles(all_candles)
    print("Done") # update the message or delete it
    return df


def fetch_incremental(last_date):
    '''Fetch only new data since last date'''
    print("Fetching last date data")
    to_ts = int(datetime.datetime.now().timestamp())
    candles = fetch_chunk(to_ts)
    df = parse_candles(candles)
    df = df[df.index > last_date]
    print(f"{len(df)} new days fetched.")
    return df


def load_data(output_path="data/raw/btc_daily.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)  # look more into this line and the whole os library 

    if os.path.exists(output_path):
        print(f"Existing data found at {output_path}")

        existing_df = pd.read_csv(output_path, index_col="timestamp", parse_dates=True)
        last_date = existing_df.index.max()  # double check if I can do this, if anything I will switch it back
        new_df = fetch_incremental(last_date)
        
        if new_df.empty:
            print("The current df is up to date")
            return existing_df

        df = pd.concat([existing_df, new_df])
        df = df[~df.index.duplicated(keep="last")]
    else:
        df = fetch_all_historical()

    df.sort_index(inplace=True)
    df.to_csv(output_path)
    print(f"Data saved to {output_path}") # update this output message
    return df

if __name__=="__main__":
    df=load_data()
    print(df.tail())
