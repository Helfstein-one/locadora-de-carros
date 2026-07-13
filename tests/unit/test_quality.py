import pytest
import os
import json
from src.quality.validator import PySparkDataQualityValidator

def test_validator_drops_nulls_and_generates_report(spark, tmp_path):
    report_path = os.path.join(tmp_path, "report.json")
    validator = PySparkDataQualityValidator(report_path)
    
    data = [
        ("2023-01-01 10:00:00", "sale", "addr1", "100.0", "SP", "10.0"),
        (None, "sale", "addr2", "100.0", "SP", "10.0"), # Falta timestamp
        ("2023-01-02 10:00:00", "rent", None, "100.0", "SP", "10.0"), # Falta addr
        ("2023-01-03 10:00:00", "sale", "addr3", "invalido", "SP", "10.0"), # Vai virar null no cast de double
    ]
    df = spark.createDataFrame(data, ["timestamp", "transaction_type", "receiving_address", "amount", "location_region", "risk_score"])
    
    clean_df = validator.validate(df)
    
    assert clean_df.count() == 1
    
    assert os.path.exists(report_path)
    with open(report_path, "r") as f:
        report = json.load(f)
        assert report["total_records_input"] == 4
        assert report["total_records_valid"] == 1
        assert report["errors"] == 3
