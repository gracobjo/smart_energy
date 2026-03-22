"""
DAG KDD Fase I: Ingesta. Ejecuta producer.py (Electricity Maps + OpenWeather + Kafka + HDFS).
Reutiliza el mismo código que el dashboard y scripts manuales.
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
PYTHON = "python3"
PRODUCER = BASE / "producer.py"

default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": __import__("datetime").timedelta(minutes=2),
}

with DAG(
    dag_id="dag_kdd_fase1_ingesta_smart_grid",
    default_args=default_args,
    description="KDD Fase I: Ingesta producer.py → Kafka + HDFS",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart_grid", "kdd", "fase1", "ingesta"],
) as dag:

    ingesta = BashOperator(
        task_id="ejecutar_ingesta",
        bash_command=f"cd {BASE} && {PYTHON} {PRODUCER}",
        env={"BASE": str(BASE)},
    )
