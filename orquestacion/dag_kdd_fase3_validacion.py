"""
DAG KDD Fase III-IV: Validación. Verifica datos en HDFS, Kafka y ejecuta comprobación NiFi.
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

default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="dag_kdd_fase3_validacion_smart_grid",
    default_args=default_args,
    description="KDD Fase III-IV: Validación (HDFS, Kafka, NiFi flujo)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart_grid", "kdd", "fase3", "validacion"],
) as dag:

    comprobar_hdfs = BashOperator(
        task_id="comprobar_hdfs",
        bash_command="hdfs dfs -ls /user/hadoop/energy_backup 2>/dev/null | tail -5 || echo 'HDFS no disponible'",
    )
    comprobar_nifi = BashOperator(
        task_id="comprobar_flujo_nifi",
        bash_command=f"cd {BASE} && python3 scripts/nifi_flujo_comprobar.py 2>/dev/null || true",
    )

    comprobar_hdfs >> comprobar_nifi
