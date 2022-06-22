import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import from_unixtime, col, to_timestamp
from pyspark.sql.types import DoubleType
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
# Create spark session
spark = (SparkSession
    .builder
    .getOrCreate()
)
####################################
# Parameters
####################################
STORAGE_ACCOUNT_NAME = sys.argv[1]
STORAGE_ACCOUNT_ACCESS_KEY = sys.argv[2]

AZURE_CONTAINER_NAME = sys.argv[3]

AZURE_SQL_SERVER_NAME = sys.argv[4]
AZURE_SQL_SERVER_DATABASE = sys.argv[5]
AZURE_SQL_SERVER_USERNAME = sys.argv[6]
AZURE_SQL_SERVER_PASSWORD = sys.argv[7]
AZURE_SQL_SERVER_PORT = sys.argv[8]

AZURE_BLOB_CONNECTION_STRING = sys.argv[9]

AZURE_SQL_SERVER_URL = f"jdbc:postgresql://{AZURE_SQL_SERVER_NAME}/{AZURE_SQL_SERVER_DATABASE}"


spark.conf.set("fs.azure.account.key." + STORAGE_ACCOUNT_NAME + ".blob.core.windows.net", STORAGE_ACCOUNT_ACCESS_KEY)

blob_container = "2022-06-21"

stocks_list_path = "wasbs://" + blob_container + "@" + STORAGE_ACCOUNT_NAME + ".blob.core.windows.net/stock_list.csv"
print(stocks_list_path)
df_stocks_list = spark.read.format("csv").load(stocks_list_path, inferSchema = True, header = True)

df_stocks_list.write \
    .format("jdbc") \
    .option("url", AZURE_SQL_SERVER_URL) \
    .option("dbtable", 'public.stocks_list') \
    .option("user", AZURE_SQL_SERVER_USERNAME) \
    .option("password", AZURE_SQL_SERVER_PASSWORD) \
    .mode("append") \
    .save()
    

blob_service_client = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

generator = container_client.list_blobs()

for blob in generator:
    if (blob.name == "stock_list.csv"):
        continue
    
    stock_path = "wasbs://" + blob_container + "@" + STORAGE_ACCOUNT_NAME + ".blob.core.windows.net/{}".format(blob.name)
    print(stock_path)
    df_stocks_historical = spark.read.format("csv").load(stocks_list_path, inferSchema = True, header = True)

    df_stocks_historical.write \
        .format("jdbc") \
        .option("url", AZURE_SQL_SERVER_URL) \
        .option("dbtable", 'public.stocks_historical') \
        .option("user", AZURE_SQL_SERVER_USERNAME) \
        .option("password", AZURE_SQL_SERVER_PASSWORD) \
        .mode("append") \
        .save()
    
    
    