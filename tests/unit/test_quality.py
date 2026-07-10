import pandas as pd
import pytest
import os
from src.quality.validator import PandasDataQualityValidator

def test_validator_drops_invalid_rows(tmp_path):
    report_file = tmp_path / "dq_report.json"
    validator = PandasDataQualityValidator(str(report_file))
    
    df = pd.DataFrame({
        "timestamp": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "transaction_type": ["sale", "sale", "sale"],
        "receiving address": ["A", "B", "C"],
        "amount": [10.0, "invalid_amount", 30.0],
        "location_region": ["SP", "RJ", "MG"],
        "risk score": [1.0, 2.0, None]
    })
    
    result = validator.validate(df)
    
    # Apenas a primeira linha deve ser válida (as outras tem erro no tipo de amount ou score nulo)
    assert len(result) == 1
    assert result.iloc[0]["receiving address"] == "A"
    
    # Checar se o arquivo json do report foi gerado
    assert os.path.exists(report_file)

def test_validator_missing_columns():
    validator = PandasDataQualityValidator("dummy.json")
    
    df = pd.DataFrame({
        "amount": [10.0]
    })
    
    # Deve dar ValueError devido à falta de colunas esperadas
    with pytest.raises(ValueError):
        validator.validate(df)
