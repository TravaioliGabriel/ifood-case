import os
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

    # Tenta obter a sessão ativa (funciona no notebook)
    active_session = SparkSession.getActiveSession()
    
    if active_session is not None:
        # Usa a sessão ativa do notebook
        yield active_session
        # Não para a sessão - ela pertence ao notebook
    else:
        # Verifica se está em ambiente Databricks via subprocess
        is_databricks_subprocess = os.environ.get("SPARK_REMOTE") is not None
        
        if is_databricks_subprocess:
            # Em subprocess do Databricks, não conseguimos criar sessão
            # Pula testes que precisam de uma sessão Spark local
            pytest.skip("Teste requer sessão Spark local, mas está em subprocess do Databricks")
        else:
            # Em ambiente local, cria uma sessão local
            session = (
                SparkSession.builder.master("local[1]")
                .appName("ifood-case-tests")
                .config("spark.sql.shuffle.partitions", "1")
                .getOrCreate()
            )
            yield session
            session.stop()
