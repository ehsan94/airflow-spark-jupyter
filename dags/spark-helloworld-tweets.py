from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator
###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
parc_files = "/opt/spark/resources/data/parc/"

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-hello-world-tweet", 
        description="This DAG runs a Pyspark .",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="spark_job",
    application="/opt/spark/app/hello-world-tweet.py", # Spark application path created in airflow and spark cluster
    name="hello-world-tweet",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":spark_master},
    application_args=[parc_files],
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end