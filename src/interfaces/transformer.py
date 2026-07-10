from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class TransformerInterface(ABC):
    @abstractmethod
    def transform_risk_score(self, df: DataFrame) -> DataFrame:
        """Gera a tabela 1: location_region ordenada por media de risk score."""
        pass

    @abstractmethod
    def transform_top_sales(self, df: DataFrame) -> DataFrame:
        """Gera a tabela 2: top 3 receiving address mais recentes de vendas."""
        pass
