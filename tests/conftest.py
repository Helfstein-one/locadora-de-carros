import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Fixture para prover a SparkSession aos testes uma única vez por run."""
    spark = SparkSession.builder \
        .appName("LocadoraTests") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()
