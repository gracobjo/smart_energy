"""
DAG informes de todas las fases KDD. Genera un informe consolidado (Markdown/JSON)
con el estado de: servicios, ingesta (HDFS, Kafka), procesamiento (Cassandra),
Hive, NiFi.
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

SCRIPT = BASE / "scripts" / "generar_informe_fases.py"
REPORTS = BASE / "reports"

default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="dag_informes_fases_smart_grid",
    default_args=default_args,
    description="Generar informe consolidado de todas las fases KDD",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart_grid", "informes", "kdd", "fases"],
) as dag:

    generar_informe = BashOperator(
        task_id="generar_informe_fases",
        bash_command=(
            f"cd {BASE} && python3 {SCRIPT} --format md --quick && "
            f"python3 {SCRIPT} --format json -o {REPORTS}/informe_fases_latest.json --quick"
        ),
    )
