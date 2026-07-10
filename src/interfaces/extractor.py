from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class ExtractorInterface(ABC):
    @abstractmethod
    def extract(self) -> DataFrame:
        """Extrai os dados e retorna como um DataFrame do PySpark."""
        pass
