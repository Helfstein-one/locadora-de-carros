import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, desc, row_number
from pyspark.sql.window import Window
from src.interfaces.transformer import TransformerInterface

logger = logging.getLogger(__name__)

class BusinessTransformer(TransformerInterface):
    def transform_risk_score(self, df: DataFrame) -> DataFrame:
        logger.info("Executando cálculo de risk score médio por região (PySpark)...")
        return df.groupBy("location_region") \
                 .agg(avg("risk_score").alias("risk_score")) \
                 .orderBy(desc("risk_score"))

    def transform_top_sales(self, df: DataFrame) -> DataFrame:
        logger.info("Executando cálculo do top 3 vendas recentes (PySpark Window Functions)...")
        sales_df = df.filter(col("transaction_type") == "sale")
        
        window_spec = Window.partitionBy("receiving_address").orderBy(desc("timestamp"))
        
        recent_sales_df = sales_df.withColumn("rn", row_number().over(window_spec)) \
                                  .filter(col("rn") == 1) \
                                  .drop("rn")
                                  
        top_3 = recent_sales_df.orderBy(desc("amount")).limit(3)
        return top_3.select("receiving_address", "amount", "timestamp")
