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
    
    input_path = os.path.join(os.getcwd(), "data", "input", "df_fraud_credit.csv.gz")
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
    
    logger.info("=== RESULTADOS FINAIS ===")
    import json
    
    # 1. Reporte de DQ
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            dq_report = json.load(f)
            logger.info("1. Reporte de Qualidade de Dados (Data Quality):")
            logger.info(json.dumps(dq_report, indent=4))
    except Exception as e:
        logger.error(f"Não foi possível ler o dq_report: {e}")
        
    # 2. Risk Score por Região
    try:
        risk_path = os.path.join(output_dir, "risk_score_por_regiao.csv")
        logger.info("2. Output: Risk Score por Região")
        df_risk = spark.read.csv(risk_path, header=True, inferSchema=True)
        df_risk.show(truncate=False)
    except Exception as e:
        logger.error(f"Não foi possível ler risk_score_por_regiao: {e}")

    # 3. Top 3 Sales Recentes
    try:
        top_3_path = os.path.join(output_dir, "top_3_sales_recentes.csv")
        logger.info("3. Output: Top 3 Vendas Recentes")
        df_top3 = spark.read.csv(top_3_path, header=True, inferSchema=True)
        df_top3.show(truncate=False)
    except Exception as e:
        logger.error(f"Não foi possível ler top_3_sales_recentes: {e}")
        
    spark.stop()
    logger.info("Processamento finalizado com sucesso.")
