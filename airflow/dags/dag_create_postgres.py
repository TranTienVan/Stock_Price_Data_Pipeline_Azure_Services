import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

with DAG(
    dag_id="create_postgres_database",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    create_stocks_list_table = PostgresOperator(
        task_id="create_stocks_list_table",
        postgres_conn_id="postgres-db",
        sql="""
            CREATE TABLE stocks_list
            (
                symbol varchar(50),
                isin varchar(50),
                name varchar(200),
                full_name varchar(200),
                tag varchar(200),
                
                primary key(symbol)
            )
          """,
    )
    
    create_stocks_historical_table = PostgresOperator(
        task_id="create_stocks_historical_table",
        postgres_conn_id="postgres-db",
        sql="""
            CREATE TABLE stocks_historical
            (
                symbol varchar(10),
                date date,
                open float,
                high float,
                low float,
                close float,
                volume bigint,
                market_cap bigint,
                sma_50 float,
                sma_200 float,
                "returns" float,
        
                primary key(symbol, date),
                foreign key (symbol) references stocks_list(symbol)
            )
          """,
    )
    
    add_sector_industry_stocks_list = PostgresOperator(
        task_id="add_sector_industry_stocks_list",
        postgres_conn_id="postgres-db",
        sql="""
            ALTER TABLE stocks_list 
            ADD COLUMN industry varchar(100),
            ADD COLUMN sector varchar(100)
          """,
    )
    create_stocks_list_table >> create_stocks_historical_table >> add_sector_industry_stocks_list