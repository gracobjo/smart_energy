#!/usr/bin/env python3
"""
Re-entrenamiento del modelo que estima cuándo la red necesitará energía de respaldo.
Usa histórico en HDFS (energy_*.json): correlación carga vs intensidad de carbono (Electricity Maps).
Escribe umbrales en Cassandra (tabla modelo_respaldo) para alertas operativas.
Ejecutado mensualmente vía Airflow tras limpieza HDFS y grafos.
"""
import json
import os
import sys
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Dict, Any

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from config import HDFS_BACKUP_PATH, CASSANDRA_HOST, KEYSPACE


def _leer_muestras_hdfs_local_fallback(max_files: int = 80) -> List[Dict]:
    """Si HDFS no disponible, entrena con umbrales por defecto."""
    import subprocess
    muestras = []
    try:
        r = subprocess.run(
            ["hdfs", "dfs", "-ls", HDFS_BACKUP_PATH],
            capture_output=True,
            text=True,
            timeout=20,
        )
        if r.returncode != 0:
            return []
        paths = [ln.split()[-1] for ln in r.stdout.strip().split("\n") if "energy_" in ln and ".json" in ln]
        for p in paths[-max_files:]:
            rr = subprocess.run(["hdfs", "dfs", "-cat", p], capture_output=True, text=True, timeout=30)
            if rr.returncode == 0 and rr.stdout:
                try:
                    muestras.append(json.loads(rr.stdout.strip().split("\n")[0] if "\n" in rr.stdout else rr.stdout))
                except Exception:
                    pass
    except Exception:
        pass
    return muestras


def entrenar_y_persistir() -> Dict[str, Any]:
    muestras = _leer_muestras_hdfs_local_fallback()
    usos = []
    carbonos = []
    for m in muestras:
        em = m.get("electricity_maps") or {}
        ci = em.get("carbon_intensity_g_co2_kwh")
        if ci is not None:
            carbonos.append(float(ci))
        for s in (m.get("subestaciones") or {}).values():
            u = s.get("uso_pct")
            if u is not None:
                usos.append(float(u))

    if len(usos) < 5:
        umbral_carga = 82.0
        umbral_carbono = 280.0
        prob = 0.15
    else:
        usos.sort()
        umbral_carga = usos[int(len(usos) * 0.85)]
        umbral_carbono = sum(carbonos) / len(carbonos) if carbonos else 250.0
        umbral_carbono = min(max(umbral_carbono, 150), 400)
        prob = min(0.35, max(0.08, (100 - umbral_carga) / 200.0))

    version = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    row = {
        "id": "respaldo_v1",
        "umbral_carga_media_pct": float(umbral_carga),
        "umbral_carbono_g_kwh": float(umbral_carbono),
        "prob_respaldo_umbral": float(prob),
        "version": version,
    }

    try:
        from cassandra.cluster import Cluster
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(KEYSPACE)
        session.execute(
            """
            INSERT INTO modelo_respaldo (id, umbral_carga_media_pct, umbral_carbono_g_kwh, prob_respaldo_umbral, version, ultima_actualizacion)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                row["id"],
                row["umbral_carga_media_pct"],
                row["umbral_carbono_g_kwh"],
                row["prob_respaldo_umbral"],
                row["version"],
                datetime.now(timezone.utc),
            ),
        )
        cluster.shutdown()
        print(f"[MODELO_RESPALDO] Persistido Cassandra: umbral_carga={umbral_carga:.1f}% carbono={umbral_carbono:.0f} g/kWh")
    except Exception as e:
        print(f"[MODELO_RESPALDO] Cassandra: {e} (umbrales calculados en memoria)")
    out_path = BASE / "procesamiento" / "modelo_respaldo.json"
    out_path.write_text(json.dumps(row, indent=2), encoding="utf-8")
    return row


if __name__ == "__main__":
    entrenar_y_persistir()
