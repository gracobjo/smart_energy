"""
DAG consultas Hive y Cassandra. Ejecuta consultas de ejemplo para verificar datos.
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
    dag_id="dag_consultas_hive_cassandra_smart_grid",
    default_args=default_args,
    description="Consultas de ejemplo a Hive y Cassandra",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart_grid", "consultas", "hive", "cassandra"],
) as dag:

    consulta_hive = BashOperator(
        task_id="consulta_hive",
        bash_command="source " + str(BASE / "scripts/env_smart_grid.sh") + " 2>/dev/null; "
        "spark-sql -e 'SHOW DATABASES;' 2>/dev/null || "
        "hive -e 'SHOW DATABASES;' 2>/dev/null || echo 'Hive no disponible'",
        env={**dict(os.environ), "BASE": str(BASE)},
    )
    consulta_cassandra = BashOperator(
        task_id="consulta_cassandra",
        bash_command=f"cd {BASE} && (./cassandra/bin/cqlsh -e 'DESCRIBE KEYSPACE smart_grid' 127.0.0.1 9042 2>/dev/null || "
        "cqlsh -e 'DESCRIBE KEYSPACE smart_grid' 127.0.0.1 9042 2>/dev/null || echo 'Cassandra no disponible')",
    )

    [consulta_hive, consulta_cassandra]
