"""
DAG comprobar servicios Smart Grid. Reutiliza scripts/comprobar_servicios.sh.
Verifica HDFS, Kafka, Cassandra, NiFi, Airflow.
Ejecución: manual o cada hora.
"""
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

_BASE = Path(__file__).resolve().parent.parent
BASE = Path(os.environ.get("SMART_ENERGY_HOME", str(_BASE)))
if BASE.name == "airflow" or not (BASE / "scripts" / "comprobar_servicios.sh").exists():
    BASE = Path("/home/hadoop/smart_energy")

default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="dag_comprobar_servicios_smart_grid",
    default_args=default_args,
    description="Comprobar HDFS, Kafka, Cassandra, NiFi, Airflow (scripts/comprobar_servicios.sh)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart_grid", "servicios", "comprobar"],
) as dag:

    comprobar = BashOperator(
        task_id="comprobar_servicios",
        bash_command=f"cd {BASE} && bash scripts/comprobar_servicios.sh",
    )
