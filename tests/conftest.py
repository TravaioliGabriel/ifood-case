import sys
from pathlib import Path

import pytest

# permite importar src/pipeline_utils.py nos testes
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

try:
    from pyspark.sql import SparkSession

    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False


@pytest.fixture(scope="session")
def spark():
    if not HAS_PYSPARK:
        pytest.skip("pyspark não instalado")

    session = (
        SparkSession.builder.master("local[1]")
        .appName("ifood-case-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()
