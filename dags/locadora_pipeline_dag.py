from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import logging

# Ajustar o PYTHONPATH para encontrar a pasta src
sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.dirname(__file__))))

from src.pipeline import LocadoraPipeline
from src.extractor.csv_extractor import CSVExtractor
from src.quality.validator import PySparkDataQualityValidator
from src.transformer.business_transformer import BusinessTransformer
from src.loader.csv_loader import SparkCSVLoader
from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

def execute_pipeline():
    input_path = "/opt/airflow/data/input/df_fraud_credit.csv.gz"
    report_path = "/opt/airflow/data/reports/dq_report.json"
    output_dir = "/opt/airflow/data/output"
    
    spark = SparkSession.builder \
        .appName("LocadoraAirflowApp") \
        .master("local[*]") \
        .getOrCreate()

    pipeline = LocadoraPipeline(
        extractor=CSVExtractor(spark, input_path),
        validator=PySparkDataQualityValidator(report_path),
        transformer=BusinessTransformer(),
        loader=SparkCSVLoader(output_dir)
    )
    
    pipeline.run()
    spark.stop()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'locadora_pipeline_dag',
    default_args=default_args,
    description='Pipeline de Processamento de Dados da Locadora de Carros (PySpark)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['locadora', 'pipeline', 'pyspark'],
) as dag:

    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=execute_pipeline,
    )

    run_pipeline_task
