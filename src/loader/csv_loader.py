import logging
import os
import shutil
from pyspark.sql import DataFrame
from typing import Dict
from src.interfaces.loader import LoaderInterface

logger = logging.getLogger(__name__)

class SparkCSVLoader(LoaderInterface):
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def load(self, dataframes: Dict[str, DataFrame]) -> None:
        logger.info(f"Iniciando gravação dos dados via PySpark no diretório: {self.output_dir}")
        os.makedirs(self.output_dir, exist_ok=True)
        
        for name, df in dataframes.items():
            path = os.path.join(self.output_dir, name)
            
            # Limpa lixo antigo
            if os.path.exists(f"{path}.csv"):
                if os.path.isdir(f"{path}.csv"):
                    shutil.rmtree(f"{path}.csv")
                else:
                    os.remove(f"{path}.csv")
                    
            tmp_path = f"{path}_tmp"
            # coalesce(1) empurra pra mesma partição, resultando em um único arquivo
            df.coalesce(1).write.csv(tmp_path, header=True, mode="overwrite")
            
            # O spark gera o arquivo numa pasta. Vamos mover pra ter um .csv perfeitinho na saida
            csv_file = [f for f in os.listdir(tmp_path) if f.startswith("part-") and f.endswith(".csv")][0]
            shutil.move(os.path.join(tmp_path, csv_file), f"{path}.csv")
            shutil.rmtree(tmp_path)
            
            logger.info(f"Tabela {name} gravada com sucesso em {path}.csv")
