"""
Fixtures pytest para suite Smart Grid (PySpark local[*]).
"""
import os
import sys
from pathlib import Path

import pytest

# Raíz del repositorio en PYTHONPATH
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark_session():
    """Sesión Spark compartida (batch); sin checkpoint de streaming en tests."""
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName("pytest-smart-grid-streaming")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()
