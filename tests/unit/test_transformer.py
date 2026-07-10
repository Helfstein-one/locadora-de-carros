import pandas as pd
from src.transformer.business_transformer import BusinessTransformer

def test_transform_risk_score():
    transformer = BusinessTransformer()
    df = pd.DataFrame({
        "location_region": ["SP", "SP", "RJ", "MG"],
        "risk score": [1.0, 2.0, 5.0, 3.0]
    })
    
    result = transformer.transform_risk_score(df)
    
    assert len(result) == 3
    # Espera-se que RJ seja o primeiro (media 5.0)
    assert result.iloc[0]["location_region"] == "RJ"
    assert result.iloc[0]["avg_risk_score"] == 5.0
    
    # Espera-se que MG seja o segundo (media 3.0)
    assert result.iloc[1]["location_region"] == "MG"
    
    # Espera-se que SP seja o terceiro (media 1.5)
    assert result.iloc[2]["location_region"] == "SP"
    assert result.iloc[2]["avg_risk_score"] == 1.5

def test_transform_top_sales():
    transformer = BusinessTransformer()
    df = pd.DataFrame({
        "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03", "2023-01-04"],
        "transaction_type": ["sale", "sale", "sale", "rent"],
        "receiving address": ["A", "B", "A", "C"],
        "amount": [10.0, 50.0, 20.0, 100.0]
    })
    
    result = transformer.transform_top_sales(df)
    
    assert len(result) == 2 # Somente os endereços A e B tiveram 'sale'
    
    # 'A' tem duas vendas (10 e 20). 20 é mais recente (2023-01-03).
    # 'B' tem uma venda (50).
    # Ordenado por amount decrescente, deve ficar B(50) e A(20).
    assert result.iloc[0]["receiving address"] == "B"
    assert result.iloc[0]["amount"] == 50.0
    assert result.iloc[1]["receiving address"] == "A"
    assert result.iloc[1]["amount"] == 20.0
