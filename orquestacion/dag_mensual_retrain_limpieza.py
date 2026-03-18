"""
DAG mensual Smart Grid: re-entrenamiento del modelo de grafos y limpieza de HDFS.
- Día 1 de cada mes a las 00:00.
- Limpia en HDFS archivos energy_* más antiguos que DIAS_RETENCION_BACKUP.
- Re-ejecuta procesamiento de grafos (batch) como re-entrenamiento.
"""
from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

BASE = Path(__file__).resolve().parent.parent
HDFS_BACKUP_PATH = os.environ.get("HDFS_BACKUP_PATH", "/user/hadoop/energy_backup")
DIAS_RETENCION_BACKUP = int(os.environ.get("DIAS_RETENCION_BACKUP", "30"))


def limpiar_hdfs_temporales(**context):
    import subprocess
    from datetime import datetime, timedelta
    cutoff = (datetime.utcnow() - timedelta(days=DIAS_RETENCION_BACKUP)).strftime("%Y%m%d")
    r = subprocess.run(
        ["hdfs", "dfs", "-ls", HDFS_BACKUP_PATH],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if r.returncode != 0:
        return
    for line in (r.stdout or "").strip().split("\n"):
        if "energy_" not in line:
            continue
        parts = line.split()
        if len(parts) < 8:
            continue
        path = parts[-1]
        try:
            name = path.split("/")[-1]
            if name.startswith("energy_") and ".json" in name:
                # energy_20260318_123456.json -> 20260318
                fecha_str = name.replace("energy_", "").split("_")[0][:8]
                if len(fecha_str) == 8 and fecha_str < cutoff:
                    subprocess.run(["hdfs", "dfs", "-rm", "-skipTrash", path], capture_output=True, timeout=10)
        except Exception:
            pass


def _python_bin():
    for v in (BASE / "venv_transporte" / "bin" / "python", BASE / "venv" / "bin" / "python"):
        if v.exists():
            return str(v)
    return "python3"


def reentrenar_grafos(**context):
    import subprocess
    script = BASE / "procesamiento" / "procesamiento_grafos.py"
    r = subprocess.run(
        [_python_bin(), str(script)],
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=300,
        env={"REENTRENAMIENTO_MENSUAL": "1", **os.environ},
    )
    if r.returncode != 0:
        raise RuntimeError(f"Re-entrenamiento grafos falló: {r.stderr or r.stdout}")


def reentrenar_modelo_respaldo(**context):
    """Modelo que predice necesidad de energía de respaldo (umbrales desde histórico HDFS + carbono)."""
    import subprocess
    script = BASE / "procesamiento" / "modelo_respaldo_energia.py"
    r = subprocess.run(
        [_python_bin(), str(script)],
        cwd=str(BASE),
        capture_output=True,
        text=True,
        timeout=120,
        env=os.environ,
    )
    if r.returncode != 0:
        raise RuntimeError(f"Re-entrenamiento modelo respaldo falló: {r.stderr or r.stdout}")


default_args = {
    "owner": "smart_grid",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_mensual_retrain_limpieza_smart_grid",
    default_args=default_args,
    description="Smart Grid - Limpieza HDFS, re-entrenamiento grafos y modelo respaldo",
    schedule="0 0 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["smart_grid", "mensual", "limpieza", "retrain"],
) as dag:

    limpieza_hdfs = PythonOperator(
        task_id="limpiar_hdfs_temporales",
        python_callable=limpiar_hdfs_temporales,
    )
    reentrenar_grafos_op = PythonOperator(
        task_id="reentrenar_modelo_grafos",
        python_callable=reentrenar_grafos,
    )
    reentrenar_respaldo = PythonOperator(
        task_id="reentrenar_modelo_respaldo_energia",
        python_callable=reentrenar_modelo_respaldo,
    )
    limpieza_hdfs >> reentrenar_grafos_op >> reentrenar_respaldo
