import investpy
import pandas as pd
import os
import pandasql as ps
import requests
from bs4 import BeautifulSoup
import time
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

import azure

def retrieve(conn, table: str, columns: list):
    cursor = conn.cursor()
        
    cursor.execute(f"select {','.join(columns)} from {table}")
    
    data = cursor.fetchall()
    cols = []
    for elt in cursor.description:
        cols.append(elt[0])
    
    df = pd.DataFrame(data = data, columns = cols)
    
    cursor.close()
    return df

def retrieve_condition(conn, table: str, columns: list, condition: dict):
    cursor = conn.cursor()
        
    cursor.execute(f"select {','.join(columns)} from {table} where {condition['key']} = %s", (condition["value"], ))
    
    data = cursor.fetchall()
    cols = []
    for elt in cursor.description:
        cols.append(elt[0])
    
    df = pd.DataFrame(data = data, columns = cols)
    
    cursor.close()
    return df
    
def extract_stocks():
    stocks = pd.read_csv("https://raw.githubusercontent.com/alvarobartt/investpy/master/investpy/resources/stocks.csv")
    stocks_vn = stocks[stocks["country"] == "vietnam"]
    
    del stocks_vn["country"]
    del stocks_vn["currency"]
    del stocks_vn["id"]
    stocks_vn = stocks_vn.reset_index()
    del stocks_vn["index"]
    
    stocks_vn = stocks_vn[["symbol", "isin", "name", "full_name", "tag"]]
    
    stocks_vn["industry"] = None
    stocks_vn["sector"] = None
    return stocks_vn



def extract_stocks_list(blob_service_client: azure.storage.blob._blob_service_client.BlobServiceClient, AZURE_CONTAINER_NAME):
    # cursor = conn.cursor()
    
    stocks_list = extract_stocks()
    industries = []
    sectors = []
    
    for symbol in stocks_list["symbol"].values:
        time.sleep(6.5)
        try:
            company = investpy.get_stock_company_profile(stock=symbol, country="vietnam")
        except:
            print("No Industry, Sector for {}".format(symbol))
            industries.append(None)
            sectors.append(None)
            continue
        url = company["url"]
        
        r = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        
        if r.status_code not in [200, 201, 202, 204, 205]:
            print("No data Industry, Sector for {}".format(symbol))
            industries.append(None)
            sectors.append(None)
            continue
        
        try:
            soup = BeautifulSoup(r.text, "html.parser")
            profile = soup.findAll("div", class_="companyProfileHeader")[0]
            
            infors = profile.findAll("a")
            
            industry = infors[0].getText()
            sector = infors[1].getText()
            
            
            print("Update industry, sector successfully {} {} {}".format(symbol, industry, sector))
            industries.append(industry)
            sectors.append(sector)
        except Exception as e:
            
            print("Error {} {}".format(e, symbol))
            industries.append(None)
            sectors.append(None)
            
    stocks_list["industry"] = industries
    stocks_list["sector"] = sectors
    
    container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)
    
    # cursor.close()
    output = stocks_list.to_csv(index_label = False, encoding = "utf-8", index=False)
    
    # Instantiate a new BlobClient
    blob_client = container_client.get_blob_client("stock_list.csv")
    # upload data
    blob_client.upload_blob(output, blob_type="BlockBlob")

    
