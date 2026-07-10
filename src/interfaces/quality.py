from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class DataQualityInterface(ABC):
    @abstractmethod
    def validate(self, df: DataFrame) -> DataFrame:
        """
        Valida o DataFrame.
        Deve gerar métricas/reports sobre anomalias e retornar apenas os dados válidos.
        """
        pass
