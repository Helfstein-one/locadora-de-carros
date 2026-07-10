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
from src.quality.validator import PandasDataQualityValidator
from src.transformer.business_transformer import BusinessTransformer
from src.loader.csv_loader import CSVLoader

logger = logging.getLogger(__name__)

def execute_pipeline():
    input_path = "/opt/airflow/data/input/data.csv"
    report_path = "/opt/airflow/data/reports/dq_report.json"
    output_dir = "/opt/airflow/data/output"
    
    # Mocking um dataset caso não exista para que o teste seja fluído
    if not os.path.exists(input_path):
        import pandas as pd
        logger.info("Criando mock dataset em data/input/data.csv pois não foi encontrado arquivo original.")
        os.makedirs(os.path.dirname(input_path), exist_ok=True)
        dummy_df = pd.DataFrame({
            "timestamp": ["2023-10-01 10:00:00", "2023-10-02 11:00:00", "2023-10-03 12:00:00", "2023-10-04 13:00:00"],
            "transaction_type": ["sale", "sale", "rent", "sale"],
            "receiving address": ["addr_1", "addr_2", "addr_1", "addr_3"],
            "amount": [1500.50, 2000.00, 300.00, 3500.00],
            "location_region": ["Sudeste", "Sul", "Nordeste", "Sudeste"],
            "risk score": [0.1, 0.5, 0.2, 0.9]
        })
        dummy_df.to_csv(input_path, index=False)

    pipeline = LocadoraPipeline(
        extractor=CSVExtractor(input_path),
        validator=PandasDataQualityValidator(report_path),
        transformer=BusinessTransformer(),
        loader=CSVLoader(output_dir)
    )
    pipeline.run()

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
    description='Pipeline de Processamento de Dados da Locadora de Carros',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['locadora', 'pipeline'],
) as dag:

    run_pipeline_task = PythonOperator(
        task_id='run_pipeline',
        python_callable=execute_pipeline,
    )

    run_pipeline_task
