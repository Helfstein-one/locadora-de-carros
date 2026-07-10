from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict

class LoaderInterface(ABC):
    @abstractmethod
    def load(self, dataframes: Dict[str, pd.DataFrame]) -> None:
        """Carrega (salva) os dataframes nos destinos configurados."""
        pass
