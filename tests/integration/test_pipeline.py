import os
import pandas as pd
import pytest
import json
from src.pipeline import LocadoraPipeline
from src.extractor.csv_extractor import CSVExtractor
from src.quality.validator import PandasDataQualityValidator
from src.transformer.business_transformer import BusinessTransformer
from src.loader.csv_loader import CSVLoader

def test_full_pipeline_execution(tmp_path):
    """
    Testa o fluxo ponta a ponta do pipeline:
    Extração -> Validação -> Transformação -> Carga.
    """
    # Setup de diretórios temporários para o teste
    input_dir = tmp_path / "data" / "input"
    output_dir = tmp_path / "data" / "output"
    reports_dir = tmp_path / "data" / "reports"
    
    input_dir.mkdir(parents=True)
    
    input_file = input_dir / "data.csv"
    report_file = reports_dir / "dq_report.json"
    
    # 1. Criação do Dataset Mockado (Input)
    df = pd.DataFrame({
        "timestamp": ["2023-01-01 10:00:00", "2023-01-02 11:00:00", "2023-01-03 12:00:00", "2023-01-04 13:00:00", "2023-01-05 14:00:00"],
        "transaction_type": ["sale", "sale", "rent", "sale", "sale"],
        "receiving address": ["addr_1", "addr_2", "addr_1", "addr_3", "addr_1"],
        "amount": [1500.0, 2000.0, 300.0, 3500.0, "invalid_amount"], # addr_1 terá a última venda inválida para testar DQ
        "location_region": ["SP", "RJ", "SP", "RJ", "SP"],
        "risk score": [0.1, 0.5, 0.2, 0.9, 0.5]
    })
    df.to_csv(input_file, index=False)
    
    # 2. Instanciação e injeção de dependências
    pipeline = LocadoraPipeline(
        extractor=CSVExtractor(str(input_file)),
        validator=PandasDataQualityValidator(str(report_file)),
        transformer=BusinessTransformer(),
        loader=CSVLoader(str(output_dir))
    )
    
    # 3. Execução do pipeline
    pipeline.run()
    
    # 4. Verificação do Relatório de Qualidade de Dados
    assert os.path.exists(report_file), "Relatório de Data Quality não foi gerado."
    with open(report_file, 'r') as f:
        report = json.load(f)
        assert report["total_records_input"] == 5
        assert report["errors"] == 1 # A última linha tem amount inválido
        assert report["total_records_valid"] == 4
        
    # 5. Verificação das Tabelas Finais (Outputs)
    out_risk = output_dir / "risk_score_por_regiao.csv"
    out_sales = output_dir / "top_3_sales_recentes.csv"
    
    assert os.path.exists(out_risk), "Tabela 1 (Risk Score) não foi gerada."
    assert os.path.exists(out_sales), "Tabela 2 (Top Sales) não foi gerada."
    
    df_risk = pd.read_csv(out_risk)
    df_sales = pd.read_csv(out_sales)
    
    # Assertivas de negócio: Tabela 1
    # Registros válidos: RJ (0.5, 0.9 -> média 0.7), SP (0.1, 0.2 -> média 0.15)
    assert len(df_risk) == 2
    assert df_risk.iloc[0]["location_region"] == "RJ"
    assert df_risk.iloc[0]["avg_risk_score"] == 0.7
    
    # Assertivas de negócio: Tabela 2
    # Vendas válidas:
    # addr_1: 1500 (em 01/01)
    # addr_2: 2000 (em 02/01)
    # addr_3: 3500 (em 04/01)
    # Top 3 em ordem de valor (addr_3, addr_2, addr_1)
    assert len(df_sales) == 3
    assert df_sales.iloc[0]["receiving address"] == "addr_3"
    assert df_sales.iloc[0]["amount"] == 3500.0
    assert df_sales.iloc[1]["receiving address"] == "addr_2"
    assert df_sales.iloc[1]["amount"] == 2000.0
    assert df_sales.iloc[2]["receiving address"] == "addr_1"
    assert df_sales.iloc[2]["amount"] == 1500.0
