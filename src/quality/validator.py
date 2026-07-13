import logging
import json
import os
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, isnan
from src.interfaces.quality import DataQualityInterface

logger = logging.getLogger(__name__)

class PySparkDataQualityValidator(DataQualityInterface):
    def __init__(self, report_path: str):
        self.report_path = report_path

    def validate(self, df: DataFrame) -> DataFrame:
        logger.info("Iniciando validação de Data Quality com PySpark...")
        
        total_records = df.count()
        
        # Tipagem
        df = df.withColumn("amount", col("amount").cast("double")) \
               .withColumn("timestamp", col("timestamp").cast("timestamp")) \
               .withColumn("risk_score", col("risk_score").cast("double"))

        # Checagem de nulos
        null_counts = {}
        for c, t in df.dtypes:
            if t in ("double", "float"):
                condition = col(c).isNull() | isnan(col(c))
            else:
                condition = col(c).isNull()
            null_count = df.filter(condition).count()
            null_counts[c] = int(null_count)
            
        # Dropar registros inválidos nas colunas principais
        df_clean = df.na.drop(subset=["timestamp", "receiving_address", "amount", "transaction_type", "risk_score"])
        
        # Expurgar anomalias de tipagem e mapeamento corrompido (ex: região "0")
        df_clean = df_clean.filter(col("location_region") != "0")
        
        total_clean = df_clean.count()
        dropped_records = total_records - total_clean
        
        report = {
            "total_records_input": int(total_records),
            "total_records_valid": int(total_clean),
            "errors": int(dropped_records),
            "compliance_percentage": float(round((total_clean / total_records) * 100, 2)) if total_records > 0 else 0.0,
            "nulls_per_column": null_counts
        }
        
        os.makedirs(os.path.dirname(self.report_path), exist_ok=True)
        with open(self.report_path, "w") as f:
            json.dump(report, f, indent=4)
            
        logger.info(f"Data Quality validada. Conformidade: {report['compliance_percentage']}%")
        return df_clean
