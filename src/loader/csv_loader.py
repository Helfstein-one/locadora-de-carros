import pandas as pd
import logging
import os
from typing import Dict
from src.interfaces.loader import LoaderInterface

logger = logging.getLogger(__name__)

class CSVLoader(LoaderInterface):
    def __init__(self, output_dir: str = "data/output"):
        self.output_dir = output_dir

    def load(self, dataframes: Dict[str, pd.DataFrame]) -> None:
        os.makedirs(self.output_dir, exist_ok=True)
        for name, df in dataframes.items():
            file_path = os.path.join(self.output_dir, f"{name}.csv")
            try:
                df.to_csv(file_path, index=False)
                logger.info(f"Tabela '{name}' salva com sucesso em: {file_path}")
            except Exception as e:
                logger.error(f"Erro ao salvar tabela '{name}': {e}")
                raise
