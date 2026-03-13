from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="bank_tx_etl_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 3, 11),
    schedule="@daily",
    catchup=False
) as dag:

    ingest_csv = BashOperator(
        task_id="ingest_csv",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --jars /opt/jars/postgresql.jar \
          /opt/spark_jobs/ingest_csv.py \
          --input /opt/data/paysim.csv
        """
    )

    quality_check = BashOperator(
        task_id="quality_check",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.executor.memory=1g \
          --conf spark.driver.memory=1g \
          --conf spark.executor.cores=1 \
          --jars /opt/jars/postgresql.jar \
          /opt/spark_jobs/quality_check.py
        """
    )

    transform = BashOperator(
        task_id="transform",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.executor.memory=1g \
          --conf spark.driver.memory=1g \
          --conf spark.executor.cores=1 \
          --conf spark.sql.shuffle.partitions=2 \
          --jars /opt/jars/postgresql.jar \
          /opt/spark_jobs/transform.py \
          --staging /opt/staging/transactions_parquet
        """
    )

    aggregate = BashOperator(
        task_id="aggregate",
        bash_command="""
        docker exec spark-master /opt/spark/bin/spark-submit \
          --master spark://spark-master:7077 \
          --conf spark.executor.memory=1g \
          --conf spark.driver.memory=1g \
          --conf spark.executor.cores=1 \
          --conf spark.sql.shuffle.partitions=2 \
          --jars /opt/jars/postgresql.jar \
          /opt/spark_jobs/aggregate.py
        """
    )

    ingest_csv >> quality_check >> transform >> aggregate