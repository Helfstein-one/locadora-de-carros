import pandas as pd
import logging
import json
import os
from src.interfaces.quality import DataQualityInterface

logger = logging.getLogger(__name__)

class PandasDataQualityValidator(DataQualityInterface):
    def __init__(self, report_path: str = "data/reports/dq_report.json"):
        self.report_path = report_path

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando Data Quality Check")
        total_records = len(df)
        
        if total_records == 0:
            logger.warning("DataFrame vazio!")
            return df
            
        # Contagem de nulos
        null_counts = df.isnull().sum().to_dict()
        
        # Colunas requeridas conforme as regras de negócio
        required_cols = ['timestamp', 'transaction_type', 'receiving address', 'amount', 'location_region', 'risk score']
        
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"O CSV não possui as colunas necessárias: {missing_cols}")
            
        # Limpeza de nulos nas colunas chave para a análise
        df_clean = df.dropna(subset=required_cols).copy()
        
        # Padronizando tipos
        df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
        df_clean['amount'] = pd.to_numeric(df_clean['amount'], errors='coerce')
        df_clean['risk score'] = pd.to_numeric(df_clean['risk score'], errors='coerce')
        
        # Removendo linhas que falharam na conversão de tipos
        df_clean = df_clean.dropna(subset=['timestamp', 'amount', 'risk score'])
        
        dropped_records = total_records - len(df_clean)
        
        report = {
            "total_records_input": int(total_records),
            "total_records_valid": int(len(df_clean)),
            "errors": int(dropped_records),
            "compliance_percentage": float(round((len(df_clean) / total_records) * 100, 2)) if total_records > 0 else 0.0,
            "nulls_per_column": {k: int(v) for k, v in null_counts.items()}
        }
        
        # Garantindo que o diretório do report existe
        os.makedirs(os.path.dirname(self.report_path), exist_ok=True)
        
        try:
            with open(self.report_path, 'w') as f:
                json.dump(report, f, indent=4)
            logger.info(f"Reporte de DQ gerado em {self.report_path} com {report['compliance_percentage']}% de conformidade.")
        except Exception as e:
            logger.warning(f"Erro ao salvar DQ report: {e}")

        return df_clean
