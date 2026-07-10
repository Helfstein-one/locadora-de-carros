from abc import ABC, abstractmethod
import pandas as pd

class TransformerInterface(ABC):
    @abstractmethod
    def transform_risk_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gera a tabela 1: location_region ordenada por media de risk score desc."""
        pass

    @abstractmethod
    def transform_top_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gera a tabela 2: Top 3 receiving address com maior amount em sales recentes."""
        pass
