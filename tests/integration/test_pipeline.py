import pytest
import os
import json
from src.pipeline import LocadoraPipeline
from src.extractor.csv_extractor import CSVExtractor
from src.quality.validator import PySparkDataQualityValidator
from src.transformer.business_transformer import BusinessTransformer
from src.loader.csv_loader import SparkCSVLoader

def test_pipeline_integration(spark, tmp_path):
    input_path = os.path.join(tmp_path, "input.csv")
    report_path = os.path.join(tmp_path, "reports", "dq.json")
    output_dir = os.path.join(tmp_path, "output")
    
    with open(input_path, "w") as f:
        f.write("timestamp,transaction_type,receiving_address,amount,location_region,risk_score\n")
        f.write("2023-01-01T10:00:00,sale,addr_1,150.00,SP,10.0\n")
        f.write("2023-01-02T10:00:00,sale,addr_1,200.00,SP,20.0\n")
        f.write("2023-01-03T10:00:00,sale,addr_2,50.00,RJ,5.0\n")
        f.write("2023-01-03T10:00:00,rent,addr_3,500.00,RJ,5.0\n")
        f.write(",sale,addr_err,100,BA,10\n")

    pipeline = LocadoraPipeline(
        extractor=CSVExtractor(spark, input_path),
        validator=PySparkDataQualityValidator(report_path),
        transformer=BusinessTransformer(),
        loader=SparkCSVLoader(output_dir)
    )
    
    pipeline.run()
    
    # Assert JSON
    assert os.path.exists(report_path)
    with open(report_path, "r") as f:
        report = json.load(f)
        assert report["total_records_input"] == 5
        assert report["errors"] == 1
        
    assert os.path.exists(os.path.join(output_dir, "risk_score_por_regiao.csv"))
    assert os.path.exists(os.path.join(output_dir, "top_3_sales_recentes.csv"))
    
    # Check outputs
    df_risk = spark.read.csv(os.path.join(output_dir, "risk_score_por_regiao.csv"), header=True, inferSchema=True)
    res_risk = df_risk.collect()
    assert len(res_risk) == 2
    assert res_risk[0]["location_region"] == "SP"
    assert res_risk[0]["risk_score"] == 15.0 # (10+20)/2
    
    df_top = spark.read.csv(os.path.join(output_dir, "top_3_sales_recentes.csv"), header=True, inferSchema=True)
    res_top = df_top.collect()
    assert len(res_top) == 2
    assert res_top[0]["receiving_address"] == "addr_1"
    assert res_top[0]["amount"] == 200.0
