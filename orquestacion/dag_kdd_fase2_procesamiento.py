"""
DAG KDD Fase II-III: Procesamiento. Ejecuta procesamiento_grafos.py (Spark + GraphFrames → Cassandra + Hive).
Reutiliza el mismo código que el pipeline manual.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

_BASE = Path(__file__).resolve().parent.parent
BASE = Path(os.environ.get("SMART_ENERGY_HOME", str(_BASE)))
if BASE.name == "airflow":
    BASE = Path("/home/hadoop/smart_energy")
SPARK_HOME = os.environ.get("SPARK_HOME", "/opt/spark")
SCRIPT = BASE / "procesamiento" / "procesamiento_grafos.py"

default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": __import__("datetime").timedelta(minutes=3),
}

with DAG(
    dag_id="dag_kdd_fase2_procesamiento_smart_grid",
    default_args=default_args,
    description="KDD Fase II-III: Procesamiento Spark → Cassandra + Hive",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart_grid", "kdd", "fase2", "procesamiento", "spark"],
) as dag:

    procesamiento = BashOperator(
        task_id="ejecutar_procesamiento_grafos",
        bash_command=(
            f"cd {BASE} && {SPARK_HOME}/bin/spark-submit --master local "
            "--packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 "
            "procesamiento/procesamiento_grafos.py"
        ),
        env={
            "SPARK_HOME": SPARK_HOME,
            "BASE": str(BASE),
            **dict(os.environ),
        },
    )
