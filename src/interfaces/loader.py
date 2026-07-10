from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Dict

class LoaderInterface(ABC):
    @abstractmethod
    def load(self, dataframes: Dict[str, DataFrame]) -> None:
        """Carrega (salva) os dataframes nos destinos finais."""
        pass
