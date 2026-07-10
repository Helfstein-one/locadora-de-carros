from abc import ABC, abstractmethod
import pandas as pd

class ExtractorInterface(ABC):
    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extrai os dados e retorna como um DataFrame do Pandas."""
        pass
