"""
DAG parar servicios Smart Grid. Reutiliza scripts/parar_servicios.sh.
Para HDFS, Kafka, Cassandra, NiFi.
Ejecución: manual (al cerrar demo).
"""
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

_BASE = Path(__file__).resolve().parent.parent
BASE = Path(os.environ.get("SMART_ENERGY_HOME", str(_BASE)))
if BASE.name == "airflow" or not (BASE / "scripts" / "parar_servicios.sh").exists():
    BASE = Path("/home/hadoop/smart_energy")
SCRIPT = BASE / "scripts" / "parar_servicios.sh"

default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="dag_parar_servicios_smart_grid",
    default_args=default_args,
    description="Parar HDFS, Kafka, Cassandra, NiFi (scripts/parar_servicios.sh)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart_grid", "servicios", "parar"],
) as dag:

    parar = BashOperator(
        task_id="parar_servicios",
        bash_command=f"bash {SCRIPT}",
        env={"NIFI_HOME": str(BASE / "nifi-2.6.0"), "HADOOP_HOME": "/opt/hadoop"},
    )
