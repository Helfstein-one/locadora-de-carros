import pandas as pd
import logging
from src.interfaces.extractor import ExtractorInterface

logger = logging.getLogger(__name__)

class CSVExtractor(ExtractorInterface):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def extract(self) -> pd.DataFrame:
        logger.info(f"Iniciando extração do arquivo: {self.file_path}")
        try:
            df = pd.read_csv(self.file_path)
            logger.info(f"Extração concluída com sucesso. Linhas lidas: {len(df)}")
            return df
        except Exception as e:
            logger.error(f"Erro ao extrair o arquivo CSV: {e}")
            raise
