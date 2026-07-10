import os
import sys
import logging
from pyspark.sql import SparkSession

# Ajustar PYTHONPATH
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.pipeline import LocadoraPipeline
from src.extractor.csv_extractor import CSVExtractor
from src.quality.validator import PySparkDataQualityValidator
from src.transformer.business_transformer import BusinessTransformer
from src.loader.csv_loader import SparkCSVLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("LocalSparkRunner")

if __name__ == "__main__":
    logger.info("Iniciando execução local simplificada do pipeline PySpark...")
    
    input_path = os.path.join(os.getcwd(), "data", "input", "data.csv")
    report_path = os.path.join(os.getcwd(), "data", "reports", "dq_report.json")
    output_dir = os.path.join(os.getcwd(), "data", "output")
    
    spark = SparkSession.builder \
        .appName("LocadoraLocalApp") \
        .master("local[*]") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
        
    pipeline = LocadoraPipeline(
        extractor=CSVExtractor(spark, input_path),
        validator=PySparkDataQualityValidator(report_path),
        transformer=BusinessTransformer(),
        loader=SparkCSVLoader(output_dir)
    )
    
    pipeline.run()
    spark.stop()
    logger.info("Processamento finalizado. Cheque as pastas data/output e data/reports.")
