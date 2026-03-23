"""
DAG arrancar servicios Smart Grid. Reutiliza scripts/iniciar_servicios.sh.
HDFS, Kafka (KRaft), Cassandra, Airflow, calentamiento Hive y NiFi.
Ejecución: manual (Trigger DAG).
"""
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

_BASE = Path(__file__).resolve().parent.parent
# Si los DAGs están en ~/airflow/dags (copia), BASE debe ser el proyecto
BASE = Path(os.environ.get("SMART_ENERGY_HOME", os.environ.get("BASE_PATH", str(_BASE))))
if BASE.name == "airflow" or not (BASE / "scripts" / "iniciar_servicios.sh").exists():
    BASE = Path("/home/hadoop/smart_energy")
SCRIPT = BASE / "scripts" / "iniciar_servicios.sh"

default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 0,
}

with DAG(
    dag_id="dag_arranque_servicios_smart_grid",
    default_args=default_args,
    description="Arrancar HDFS, Kafka, Cassandra, Airflow y NiFi (espera 8443)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["smart_grid", "servicios", "arranque"],
) as dag:
    NIFI_HOME = BASE / "nifi-2.6.0"

    arrancar_hdfs = BashOperator(
        task_id="arrancar_hdfs",
        bash_command=f"cd {BASE} && HADOOP_HOME=${{HADOOP_HOME:-/opt/hadoop}} bash {SCRIPT} --only hdfs",
    )
    arrancar_kafka = BashOperator(
        task_id="arrancar_kafka",
        bash_command=f"cd {BASE} && KAFKA_HOME=${{KAFKA_HOME:-/opt/kafka}} bash {SCRIPT} --only kafka",
    )
    arrancar_cassandra = BashOperator(
        task_id="arrancar_cassandra",
        bash_command=f"cd {BASE} && bash {SCRIPT} --only cassandra",
    )
    arrancar_todo = BashOperator(
        task_id="arrancar_todo",
        bash_command=f"cd {BASE} && bash {SCRIPT}",
        trigger_rule="all_done",
    )

    [arrancar_hdfs, arrancar_kafka, arrancar_cassandra] >> arrancar_todo

    arrancar_nifi = BashOperator(
        task_id="arrancar_nifi",
        trigger_rule="all_done",
        env={"NIFI_HOME": str(NIFI_HOME)},
        bash_command=(
            "set -euo pipefail; "
            f"cd {BASE}; "
            "\"$NIFI_HOME/bin/nifi.sh\" start; "
            "echo 'Esperando NiFi (127.0.0.1:8443)...'; "
            "for i in {1..300}; do "
            "if (echo > /dev/tcp/127.0.0.1/8443) >/dev/null 2>&1; then "
            "echo 'NiFi OK: 127.0.0.1:8443'; "
            "exit 0; "
            "fi; "
            "sleep 2; "
            "done; "
            "echo 'ERROR: NiFi no abrió 127.0.0.1:8443'; exit 1"
        ),
    )

    arrancar_todo >> arrancar_nifi
