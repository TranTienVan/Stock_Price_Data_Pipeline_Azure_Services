# from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
# from datetime import datetime, timedelta
# from airflow.models.variable import Variable


# POSTGRES_HOST = Variable.get("POSTGRES_HOST", "/opt/airflow")
# POSTGRES_USERNAME = Variable.get("POSTGRES_USERNAME", "/opt/airflow")
# POSTGRES_NAME = Variable.get("POSTGRES_NAME", "/opt/airflow")
# POSTGRES_PASSWORD = Variable.get("POSTGRES_PASSWORD", "/opt/airflow")

# ###############################################
# # Parameters
# ###############################################
# SPARK_MASTER = Variable.get("SPARK_MASTER", "/opt/airflow")
# POSTGRES_DRIVER_JAR = Variable.get("POSTGRES_DRIVER_JAR", "/opt/airflow")

# POSTGRES_URL = f"jdbc:postgresql://{POSTGRES_HOST}/{POSTGRES_NAME}"

# ###############################################
# # DAG Definition
# ###############################################
# now = datetime.now()

# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(now.year, now.month, now.day),
#     "email": ["airflow@airflow.com"],
#     "email_on_failure": False,
#     "email_on_retry": False,
#     "retries": 1,
#     "retry_delay": timedelta(minutes=1)
# }

# dag = DAG(
#         dag_id="spark-postgres", 
#         description="This DAG is a sample of integration between Spark and DB. It reads CSV files, load them into a Postgres DB and then read them from the same Postgres DB.",
#         default_args=default_args, 
#         schedule_interval=timedelta(1)
#     )

# start = DummyOperator(task_id="start", dag=dag)

# spark_stock_price_analysis = SparkSubmitOperator(
#     task_id="spark_stock_price_analysis",
#     application="/usr/local/spark/app/stock_price_analysis.py", # Spark application path created in airflow and spark cluster
#     name="stock_price_analysis",
#     conn_id="spark_default",
#     verbose=1,
#     conf={"spark.master":SPARK_MASTER},
#     application_args=[POSTGRES_URL,POSTGRES_USERNAME,POSTGRES_PASSWORD],
#     jars=POSTGRES_DRIVER_JAR,
#     driver_class_path=POSTGRES_DRIVER_JAR,
#     dag=dag)

# end = DummyOperator(task_id="end", dag=dag)

# start >> spark_stock_price_analysis >> end