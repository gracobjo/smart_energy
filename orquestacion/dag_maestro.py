"""
Smart Grid - Orquestación Airflow 2.10.x
DAG cada 15 minutos:
- Verifica HDFS, Kafka (3.9.1 KRaft), Cassandra 5.0
- Ejecuta Ingesta (producer.py) -> Procesamiento Spark (procesamiento_grafos.py)
"""
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE = Path(__file__).resolve().parent.parent


def verificar_hdfs(**context):
    import subprocess
    r = subprocess.run(["hdfs", "dfs", "-ls", "/"], capture_output=True)
    if r.returncode != 0:
        raise RuntimeError("HDFS no disponible")
    return "OK"


def verificar_kafka(**context):
    import socket
    for srv in ["localhost:9092", "127.0.0.1:9092"]:
        host, port = srv.split(":")[0], int(srv.split(":")[1])
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            sock.connect((host, port))
            sock.close()
            return "OK"
        except Exception:
            continue
    raise RuntimeError("Kafka no disponible en localhost:9092")


def verificar_cassandra(**context):
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        sock.connect(("127.0.0.1", 9042))
        sock.close()
        return "OK"
    except Exception as e:
        raise RuntimeError(f"Cassandra no disponible: {e}")


def ejecutar_ingesta(**context):
    import subprocess
    import os
    venv_python = BASE / "venv_transporte" / "bin" / "python"
    if not venv_python.exists():
        venv_python = BASE / "venv" / "bin" / "python"
    python_bin = str(venv_python) if venv_python.exists() else "python3"
    script = BASE / "producer.py"
    r = subprocess.run(
        [python_bin, str(script)],
        env=os.environ,
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=90,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Ingesta (producer) falló: {r.stderr or r.stdout}")


def ejecutar_procesamiento(**context):
    import subprocess
    import os
    venv_python = BASE / "venv_transporte" / "bin" / "python"
    if not venv_python.exists():
        venv_python = BASE / "venv" / "bin" / "python"
    python_bin = str(venv_python) if venv_python.exists() else "python3"
    script = BASE / "procesamiento" / "procesamiento_grafos.py"
    r = subprocess.run(
        [python_bin, str(script)],
        env=os.environ,
        capture_output=True,
        text=True,
        cwd=str(BASE),
        timeout=300,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Procesamiento falló: {r.stderr or r.stdout}")


default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_maestro_smart_grid",
    default_args=default_args,
    description="Smart Grid - Ingesta y Procesamiento cada 15 min",
    schedule=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smart_grid", "energy", "spark", "kafka"],
) as dag:

    check_hdfs = PythonOperator(
        task_id="verificar_hdfs",
        python_callable=verificar_hdfs,
    )
    check_kafka = PythonOperator(
        task_id="verificar_kafka",
        python_callable=verificar_kafka,
    )
    check_cassandra = PythonOperator(
        task_id="verificar_cassandra",
        python_callable=verificar_cassandra,
    )
    ingesta = PythonOperator(
        task_id="ejecutar_ingesta",
        python_callable=ejecutar_ingesta,
    )
    procesamiento = PythonOperator(
        task_id="ejecutar_procesamiento",
        python_callable=ejecutar_procesamiento,
    )

    [check_hdfs, check_kafka, check_cassandra] >> ingesta >> procesamiento
