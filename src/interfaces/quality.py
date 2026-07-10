from abc import ABC, abstractmethod
import pandas as pd

class DataQualityInterface(ABC):
    @abstractmethod
    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Valida o DataFrame.
        Deve gerar métricas/reports e pode remover registros inválidos 
        antes de retornar o DF limpo.
        """
        pass
