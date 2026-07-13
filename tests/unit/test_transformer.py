import pytest
from src.transformer.business_transformer import BusinessTransformer

def test_transform_risk_score(spark):
    transformer = BusinessTransformer()
    data = [
        ("SP", 10.0), ("SP", 20.0),
        ("RJ", 5.0)
    ]
    df = spark.createDataFrame(data, ["location_region", "risk_score"])
    
    result_df = transformer.transform_risk_score(df)
    results = result_df.collect()
    
    assert len(results) == 2
    # SP should be first because 15 > 5
    assert results[0]["location_region"] == "SP"
    assert results[0]["risk_score"] == 15.0
    assert results[1]["location_region"] == "RJ"
    assert results[1]["risk_score"] == 5.0

def test_transform_top_sales(spark):
    transformer = BusinessTransformer()
    data = [
        ("addr1", "sale", 100.0, "2023-01-01T10:00:00"),
        ("addr1", "sale", 300.0, "2023-01-02T10:00:00"), # Mais recente pra addr1
        ("addr2", "rent", 500.0, "2023-01-01T10:00:00"), # Deve ser ignorado (rent)
        ("addr3", "sale", 50.0,  "2023-01-01T10:00:00"),
        ("addr4", "sale", 150.0, "2023-01-01T10:00:00"),
        ("addr5", "sale", 200.0, "2023-01-01T10:00:00"),
    ]
    df = spark.createDataFrame(data, ["receiving_address", "transaction_type", "amount", "timestamp"])
    
    result_df = transformer.transform_top_sales(df)
    results = result_df.collect()
    
    assert len(results) == 3
    # Top amounts should be addr1(300), addr5(200), addr4(150)
    assert results[0]["receiving_address"] == "addr1"
    assert results[0]["amount"] == 300.0
    assert results[1]["receiving_address"] == "addr5"
    assert results[2]["receiving_address"] == "addr4"
