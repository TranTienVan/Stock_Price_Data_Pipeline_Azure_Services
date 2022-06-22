import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from datetime import datetime
from load_stocks_list import extract_stocks_list
from load_stocks_historical import extract_stocks_historical
from airflow.models.variable import Variable
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from airflow.operators.dummy_operator import DummyOperator
import pyodbc


try:
    print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")

    # Quick start code goes here

except Exception as ex:
    print('Exception:')
    print(ex)

AZURE_BLOB_ACCOUNT_NAME = Variable.get("AZURE_BLOB_ACCOUNT_NAME", "/opt/airflow")
AZURE_BLOB_ACCOUNT_KEY = Variable.get("AZURE_BLOB_ACCOUNT_KEY", "/opt/airflow")

AZURE_BLOB_CONNECTION_STRING = Variable.get("AZURE_BLOB_CONNECTION_STRING", "/opt/airflow")
AZURE_CONTAINER_NAME = datetime.now().strftime("%Y-%m-%d")

AZURE_SQL_SERVER_NAME = Variable.get("AZURE_SQL_SERVER_NAME", "/opt/airflow")
AZURE_SQL_SERVER_DATABASE = Variable.get("AZURE_SQL_SERVER_DATABASE", "/opt/airflow")
AZURE_SQL_SERVER_USERNAME = Variable.get("AZURE_SQL_SERVER_USERNAME", "/opt/airflow")
AZURE_SQL_SERVER_PASSWORD = Variable.get("AZURE_SQL_SERVER_PASSWORD", "/opt/airflow")
AZURE_SQL_SERVER_PORT = Variable.get("AZURE_SQL_SERVER_PORT", "/opt/airflow")
AZURE_SQL_SERVER_DRIVER = Variable.get("AZURE_SQL_SERVER_DRIVER", "/opt/airflow")

AZURE_STORAGE_DRIVER = Variable.get("AZURE_STORAGE_DRIVER", "opt/airflow")
AZURE_HADOOP_DRIVER = Variable.get("AZURE_HADOOP_DRIVER", "opt/airflow")
SPARK_MASTER = Variable.get("SPARK_MASTER", "/opt/airflow")
blob_service_client = BlobServiceClient.from_connection_string(AZURE_BLOB_CONNECTION_STRING)

def create_container():
    container_client  = blob_service_client.create_container(AZURE_CONTAINER_NAME)

# conn = pyodbc.connect('DRIVER={};SERVER=tcp:{};PORT={};DATABASE={};UID={};PWD={}'.format(
#     AZURE_SQL_SERVER_DRIVER,
#     AZURE_SQL_SERVER_NAME,
#     AZURE_SQL_SERVER_PORT,
#     AZURE_SQL_SERVER_DATABASE,
#     AZURE_SQL_SERVER_USERNAME,
#     AZURE_SQL_SERVER_PASSWORD
# ))

default_args = {
    "owner": "airflow",
    "start_date": days_ago(2),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
dag = DAG(
    dag_id="ETL_Stock",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) 
start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)

create_container_blob_storage = PythonOperator(
    task_id="create_container_blob_storage",
    python_callable=create_container,
    dag=dag
)

drop_old_stock_historical = MsSqlOperator(
    task_id='drop_old_stock_historical',
    mssql_conn_id='stock-price-db',
    sql=r"""
    DROP TABLE IF EXISTS dbo.stocks_historical
    """,
    dag=dag
)

drop_old_stock_list = MsSqlOperator(
    task_id='drop_old_stock_list',
    mssql_conn_id='stock-price-db',
    sql=r"""
    DROP TABLE IF EXISTS dbo.stocks_list
    """,
    dag=dag
)

create_stocks_list_table = MsSqlOperator(
    task_id="create_stocks_list_table",
    mssql_conn_id="stock-price-db",
    sql="""
        CREATE TABLE stocks_list
        (
            symbol varchar(50),
            isin varchar(50),
            name varchar(200),
            full_name varchar(200),
            tag varchar(200),
            industry varchar(100),
            sector varchar(100),
            
            primary key(symbol)
        )
        """,
    dag=dag
)

create_stocks_historical_table = MsSqlOperator(
    task_id="create_stocks_historical_table",
    mssql_conn_id="stock-price-db",
    sql="""
        CREATE TABLE stocks_historical
        (
            symbol varchar(50),
            date date,
            "open" float,
            high float,
            low float,
            "close" float,
            volume bigint,
            market_cap bigint,
            sma_50 float,
            sma_200 float,
            "returns" float,
    
            primary key(symbol, date),
            foreign key (symbol) references stocks_list(symbol)
        )
        """,
    dag=dag
)

extract_stocks_list_task = PythonOperator(
    task_id="extract_stocks_list",
    python_callable=extract_stocks_list,
    op_kwargs={
        "blob_service_client": blob_service_client,
        "AZURE_CONTAINER_NAME": AZURE_CONTAINER_NAME
    },
    dag=dag
)

# TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
extract_stocks_historical_task = PythonOperator(
    task_id="extract_stocks_historical",
    python_callable=extract_stocks_historical,
    op_kwargs={
        "blob_service_client": blob_service_client,
        "AZURE_CONTAINER_NAME": AZURE_CONTAINER_NAME
    },
    dag=dag
)

spark_load_stock = SparkSubmitOperator(
    task_id="spark_load_stock",
    application="/opt/spark/app/load_stock.py", # Spark application path created in airflow and spark cluster
    name="spark_load_stock",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[AZURE_BLOB_ACCOUNT_NAME,AZURE_BLOB_ACCOUNT_KEY,AZURE_CONTAINER_NAME,AZURE_SQL_SERVER_NAME, AZURE_SQL_SERVER_DATABASE, AZURE_SQL_SERVER_USERNAME, AZURE_SQL_SERVER_PASSWORD, AZURE_SQL_SERVER_PORT, AZURE_BLOB_CONNECTION_STRING],
    jars=",".join([AZURE_SQL_SERVER_DRIVER, AZURE_HADOOP_DRIVER, AZURE_STORAGE_DRIVER]),
    driver_class_path=",".join([AZURE_SQL_SERVER_DRIVER, AZURE_HADOOP_DRIVER, AZURE_STORAGE_DRIVER]),
    dag = dag
)

spark_stock_price_analysis = SparkSubmitOperator(
    task_id="spark_stock_price_analysis",
    application="/opt/spark/app/stock_price_analysis.py", # Spark application path created in airflow and spark cluster
    name="spark_stock_price_analysis",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":SPARK_MASTER},
    application_args=[AZURE_SQL_SERVER_NAME, AZURE_SQL_SERVER_DATABASE, AZURE_SQL_SERVER_USERNAME, AZURE_SQL_SERVER_PASSWORD, AZURE_SQL_SERVER_PORT, AZURE_BLOB_CONNECTION_STRING],
    jars=AZURE_SQL_SERVER_DRIVER,
    driver_class_path=AZURE_SQL_SERVER_DRIVER,
    dag = dag
)

start >> create_container_blob_storage 
create_container_blob_storage  >> extract_stocks_list_task 
create_container_blob_storage >> extract_stocks_historical_task

start >> drop_old_stock_historical >> drop_old_stock_list >> create_stocks_list_table >> create_stocks_historical_table

create_stocks_historical_table >> spark_load_stock
extract_stocks_historical_task >> spark_load_stock
extract_stocks_list_task >> spark_load_stock

spark_load_stock >> spark_stock_price_analysis

spark_stock_price_analysis >> end