import pandas as pd
import logging
from src.interfaces.transformer import TransformerInterface

logger = logging.getLogger(__name__)

class BusinessTransformer(TransformerInterface):
    def transform_risk_score(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Gerando Tabela 1: Risk score médio por região.")
        # Agrupar por location_region e calcular a média de risk score
        risk_df = df.groupby('location_region')['risk score'].mean().reset_index()
        risk_df = risk_df.rename(columns={'risk score': 'avg_risk_score'})
        # Ordenar de forma decrescente
        risk_df = risk_df.sort_values(by='avg_risk_score', ascending=False)
        return risk_df

    def transform_top_sales(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Gerando Tabela 2: Top 3 receiving address em sales recentes.")
        # Filtrar por transaction_type == 'sale'
        sales_df = df[df['transaction_type'] == 'sale'].copy()
        
        if sales_df.empty:
            logger.warning("Nenhuma transação do tipo 'sale' encontrada.")
            return pd.DataFrame(columns=['receiving address', 'amount', 'timestamp'])

        # Ordenar por timestamp decrescente para que o 'first' seja o mais recente
        sales_df = sales_df.sort_values(by='timestamp', ascending=False)
        
        # Pegar a transação mais recente por 'receiving address'
        latest_sales = sales_df.drop_duplicates(subset=['receiving address'], keep='first')
        
        # Obter os 3 com maior 'amount'
        top_3 = latest_sales.nlargest(3, 'amount')
        
        # Selecionar apenas as colunas solicitadas
        return top_3[['receiving address', 'amount', 'timestamp']]
