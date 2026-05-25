import importlib.util
from datetime import datetime

import pytest

from pipeline_utils import DATE_END, DATE_START, SILVER_DROPNA_SUBSET, silver_sql


def test_silver_sql_yellow():
    sql = silver_sql("yellow")
    assert "vl_airport_fee" in sql
    assert DATE_START in sql
    assert DATE_END in sql


def test_silver_sql_green():
    sql = silver_sql("green")
    assert "vl_airport_fee" not in sql
    assert "lpep_pickup_datetime" in sql


@pytest.mark.skipif(
    importlib.util.find_spec("pyspark") is None,
    reason="pyspark não instalado",
)
def test_dropna_subset_mantem_linha_com_tip_nulo(spark):
    from pyspark.sql.types import DoubleType, IntegerType, StructField, StructType, TimestampType

    schema = StructType(
        [
            StructField("id_vendor", IntegerType(), True),
            StructField("pickup_datetime", TimestampType(), True),
            StructField("dropoff_datetime", TimestampType(), True),
            StructField("nr_passenger", DoubleType(), True),
            StructField("vl_total", DoubleType(), True),
            StructField("vl_tip", DoubleType(), True),
        ]
    )
    row = (1, datetime(2023, 1, 1, 10, 0), datetime(2023, 1, 1, 10, 20), 1.0, 10.0, None)
    df = spark.createDataFrame([row], schema)

    assert df.dropna(subset=SILVER_DROPNA_SUBSET).count() == 1
