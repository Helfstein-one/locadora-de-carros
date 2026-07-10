import logging
from src.interfaces.extractor import ExtractorInterface
from src.interfaces.quality import DataQualityInterface
from src.interfaces.transformer import TransformerInterface
from src.interfaces.loader import LoaderInterface

logger = logging.getLogger(__name__)

class LocadoraPipeline:
    def __init__(
        self,
        extractor: ExtractorInterface,
        validator: DataQualityInterface,
        transformer: TransformerInterface,
        loader: LoaderInterface
    ):
        self.extractor = extractor
        self.validator = validator
        self.transformer = transformer
        self.loader = loader

    def run(self):
        logger.info("Iniciando execução do Pipeline")
        
        # 1. Extract
        df_raw = self.extractor.extract()
        
        # 2. Quality & Cleanse
        df_clean = self.validator.validate(df_raw)
        
        if df_clean.isEmpty():
            logger.warning("Nenhum dado válido após limpeza. Encerrando pipeline.")
            return
            
        # 3. Transform
        df_risk_score = self.transformer.transform_risk_score(df_clean)
        df_top_sales = self.transformer.transform_top_sales(df_clean)
        
        # 4. Load
        self.loader.load({
            "risk_score_por_regiao": df_risk_score,
            "top_3_sales_recentes": df_top_sales
        })
        
        logger.info("Execução do Pipeline concluída com sucesso.")
