# import os
# from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
# import psycopg2
# from load_stocks_list import update_stocks_list, update_industry_sector
# from load_stocks_historical import update_stocks_today, update_stocks_historical
# from airflow.models.variable import Variable
# from supabase import create_client, Client



# POSTGRES_HOST = Variable.get("POSTGRES_HOST", "/opt/airflow")
# POSTGRES_USERNAME = Variable.get("POSTGRES_USERNAME", "/opt/airflow")
# POSTGRES_NAME = Variable.get("POSTGRES_NAME", "/opt/airflow")
# POSTGRES_PASSWORD = Variable.get("POSTGRES_PASSWORD", "/opt/airflow")


# SUPABASE_URL: str = Variable.get("SUPABASE_URL", "/opt/airflow")
# SUPABASE_KEY: str = Variable.get("SUPABASE_KEY", "/opt/airflow")
# supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# conn = psycopg2.connect(
#     host=POSTGRES_HOST,
#     database=POSTGRES_NAME,
#     user=POSTGRES_USERNAME,
#     password=POSTGRES_PASSWORD
# )


# default_args = {
#     "owner": "airflow",
#     "start_date": "2022-05-02",
#     "depends_on_past": False,
#     "retries": 1,
# }

# with DAG(
#     dag_id="data_update_gcs_dag",
#     schedule_interval="@daily",
#     default_args=default_args,
#     catchup=False,
#     max_active_runs=1,
#     tags=['dtc-de'],
# ) as dag:
#     stocks_list_update = PythonOperator(
#         task_id="stocks_list_update",
#         python_callable=update_stocks_list,
#         op_kwargs={
#             "conn": conn
#         },
#     )
    
#     stocks_list_update_industry_sector = PythonOperator(
#         task_id="stocks_list_update_industry_sector",
#         python_callable=update_industry_sector,
#         op_kwargs={
#             "conn": conn
#         },
#     )
    
#     stocks_historical_update = PythonOperator(
#         task_id="stocks_historical_update",
#         python_callable=update_stocks_historical,
#         op_kwargs={
#             "conn": conn
#         },
#     )
    
#     stocks_today_update = PythonOperator(
#         task_id="stocks_today_update",
#         python_callable=update_stocks_today,
#         op_kwargs={
#             "supabase": supabase,
#             "conn": conn
#         },
#     )
    
    
    
#     stocks_list_update >> stocks_historical_update >> stocks_today_update
    
#     stocks_list_update >> stocks_list_update_industry_sector