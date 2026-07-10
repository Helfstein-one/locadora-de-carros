import logging
from pyspark.sql import SparkSession, DataFrame
from src.interfaces.extractor import ExtractorInterface

logger = logging.getLogger(__name__)

class CSVExtractor(ExtractorInterface):
    def __init__(self, spark: SparkSession, file_path: str):
        self.spark = spark
        self.file_path = file_path

    def extract(self) -> DataFrame:
        logger.info(f"Iniciando extração via PySpark do arquivo: {self.file_path}")
        df = self.spark.read.csv(self.file_path, header=True, inferSchema=True)
        logger.info("Extração concluída.")
        return df
