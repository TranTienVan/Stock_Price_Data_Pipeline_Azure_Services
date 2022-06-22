
import investpy
import pandas as pd
from pendulum import yesterday
import os
import pandasql as ps
from datetime import date, timedelta
import time
from datetime import datetime, timedelta
from load_stocks_list import retrieve, retrieve_condition
import numpy as np
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
import azure

DEFAULT_START_DATE = "01/01/2016"
DEFAULT_END_DATE = "08/05/2022"
DEFAULT_COUNTRY = "Vietnam"


def transform_df(df: pd.DataFrame, symbol):
    df = df.drop_duplicates(subset = ['date'], keep = "last").reset_index(drop = True)
    df = df.sort_values(by=['date'], ascending=True)
    df["symbol"] = symbol
    df = df[["symbol", "date","open", "high", "low", "close", "volume"]]
    df["date"] = df["date"].astype(str)
    df["market_cap"] = df['open'] * df['volume']
    df["market_cap"] = df["market_cap"].astype(int)
    df['sma_50'] = df['open'].rolling(50).mean()
    df['sma_200'] = df['open'].rolling(200).mean()
    df['returns'] = (df['close']/df['close'].shift(1)) -1
    # df = df.astype(object).replace(np.nan, 'None')
    return df
def extract_load_stocks_historical(blob_service_client: azure.storage.blob._blob_service_client.BlobServiceClient, AZURE_CONTAINER_NAME, symbols_list, from_date=DEFAULT_START_DATE, to_date=DEFAULT_END_DATE):
    # cursor = conn.cursor()
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    
    for i in range(0, len(symbols_list)):
        time.sleep(7.5)
        try:
            df: pd.DataFrame = investpy.get_stock_historical_data(stock=symbols_list["symbol"].loc[i], country=DEFAULT_COUNTRY, from_date=from_date, to_date=to_date)
        except Exception as e:
            print("Error 1", symbols_list["symbol"].loc[i], e)
            continue
        
        df = df.reset_index()
        df.columns = [column.lower() for column in df.columns]
        if "currency" in df:
            del df["currency"]
        
        df = transform_df(df, symbols_list["symbol"].loc[i])
        
        # cursor.close()
        output = df.to_csv(index_label = False, encoding = "utf-8", index=False)
        
        # Instantiate a new BlobClient
        blob_client = container_client.get_blob_client("{}.csv".format(symbols_list["symbol"].loc[i]))
        # upload data
        blob_client.upload_blob(output, blob_type="BlockBlob")
        
        

def extract_stocks_historical(blob_service_client: azure.storage.blob._blob_service_client.BlobServiceClient, AZURE_CONTAINER_NAME):
    
    today = datetime.today().strftime("%d/%m/%Y")
    list_stock = investpy.get_stocks_dict(country="vietnam")
    list_stock = pd.DataFrame(list_stock)
    
    extract_load_stocks_historical(blob_service_client, AZURE_CONTAINER_NAME, list_stock, DEFAULT_START_DATE, today)
    
    
    