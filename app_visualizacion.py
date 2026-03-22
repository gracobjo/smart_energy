#!/usr/bin/env python3
"""
Dashboard Smart Grid (Streamlit + Folium).
Subestaciones, líneas HT, PageRank, puntos de fallo únicos (articulación).
"""
from __future__ import annotations

import copy
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import socket
import time
import requests

try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

BASE = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE))

import streamlit as st
import folium
from streamlit_folium import st_folium

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
except ImportError:
    Cluster = None  # type: ignore

from config_nodos import get_aristas, get_nodos
from config import (
    CASSANDRA_HOST,
    HDFS_DEFAULT_FS,
    HDFS_BACKUP_PATH,
    HIVE_DB,
    HIVE_HOME,
    KAFKA_BOOTSTRAP,
    KAFKA_HOME,
    NIFI_GPS_LOGS_DIR,
    NIFI_HOME,
    SPARK_HOME,
    TOPIC_GPS_RAW,
    TOPIC_RAW,
    TOPIC_WEATHER_RAW,
)

COLORES_ESTADO = {
    "ok": "green",
    "alerta": "orange",
    "sobrecarga": "red",
}
COLOR_DEFAULT = "gray"


def _port_open(host: str, port: int, timeout_s: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout_s):
            return True
    except Exception:
        return False


def _str_from_subprocess_chunk(chunk: Any) -> str:
    """TimeoutExpired puede exponer stdout/stderr como str o bytes."""
    if chunk is None:
        return ""
    if isinstance(chunk, bytes):
        return chunk.decode("utf-8", errors="replace")
    return str(chunk)


def _safe_run(
    cmd: List[str],
    timeout_s: int = 60,
    env: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    run_env = {**os.environ, **env} if env else None
    try:
        r = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s,
            env=run_env,
        )
        return {"rc": r.returncode, "stdout": r.stdout or "", "stderr": r.stderr or ""}
    except subprocess.TimeoutExpired as e:
        out = _str_from_subprocess_chunk(getattr(e, "stdout", None))
        err = _str_from_subprocess_chunk(getattr(e, "stderr", None))
        return {
            "rc": 124,
            "stdout": out,
            "stderr": err + f"\n[timeout {timeout_s}s] {' '.join(cmd[:8])}",
        }


# Java 21 + Hive CliDriver: StringInternUtils usa reflexión sobre java.net.URI (HIVE-29022 / módulos).
_HIVE_JAVA21_OPENS = (
    "--add-opens java.base/java.lang=ALL-UNNAMED "
    "--add-opens java.base/java.lang.reflect=ALL-UNNAMED "
    "--add-opens java.base/java.io=ALL-UNNAMED "
    "--add-opens java.base/java.net=ALL-UNNAMED "
    "--add-opens java.base/java.util=ALL-UNNAMED "
    "--add-opens java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens java.base/java.nio=ALL-UNNAMED"
)


def _hive_cli_env() -> Dict[str, str]:
    """JAVA_HOME (21), HIVE_HOME, HADOOP_HOME y PATH para hive/spark-sql (evita Hive 3 + Java 21 / Hive 4 + Java 17)."""
    e: Dict[str, str] = {}
    for jdk in (
        os.environ.get("JAVA_HOME", ""),
        "/usr/lib/jvm/java-21-openjdk-amd64",
        "/usr/lib/jvm/java-21-openjdk",
    ):
        if jdk and Path(jdk).is_dir() and (Path(jdk) / "bin" / "java").exists():
            e["JAVA_HOME"] = jdk
            break
    for cand in (
        os.environ.get("HIVE_HOME", ""),
        str(Path.home() / "apache-hive-4.2.0-bin"),
        str(Path.home() / "apache-hive-4.0.0-bin"),
        "/opt/hive",
        HIVE_HOME,
    ):
        if cand and Path(cand).joinpath("bin/hive").exists():
            e["HIVE_HOME"] = cand
            break
    for hadoop in (os.environ.get("HADOOP_HOME", ""), "/opt/hadoop"):
        if hadoop and Path(hadoop).joinpath("bin/hdfs").exists():
            e["HADOOP_HOME"] = hadoop
            break
    if os.environ.get("SPARK_HOME"):
        e["SPARK_HOME"] = os.environ["SPARK_HOME"]
    elif Path(SPARK_HOME).joinpath("bin/spark-sql").exists():
        e["SPARK_HOME"] = SPARK_HOME
    prepend: List[str] = []
    if e.get("JAVA_HOME"):
        prepend.append(f'{e["JAVA_HOME"]}/bin')
    if e.get("HIVE_HOME"):
        prepend.append(f'{e["HIVE_HOME"]}/bin')
    if e.get("SPARK_HOME"):
        prepend.append(f'{e["SPARK_HOME"]}/bin')
    elif Path("/opt/spark/bin/spark-sql").exists():
        prepend.append("/opt/spark/bin")
    out = {k: v for k, v in e.items() if v}
    if prepend:
        out["PATH"] = ":".join(prepend) + ":" + os.environ.get("PATH", "")
    # hive -e (CliDriver) con Java 21 necesita --add-opens; hive-env.sh también puede definirlos
    _hopts = (os.environ.get("HADOOP_CLIENT_OPTS", "") + " " + _HIVE_JAVA21_OPENS).strip()
    out["HADOOP_CLIENT_OPTS"] = _hopts
    return out


def _cassandra_cluster_ephemeral(contact_points: Tuple[str, ...], user: str = "", password: str = "") -> Any:
    """
    Cluster **sin** @st.cache_resource. Usar con shutdown() en finally para comprobar/aplicar esquema.
    El Cluster cacheado puede quedar «already shut down» si otra parte cerró la sesión (p. ej. session.shutdown()).
    """
    if Cluster is None:
        return None
    auth = None
    if user and password:
        auth = PlainTextAuthProvider(user, password)
    return Cluster(list(contact_points), auth_provider=auth)


def _cassandra_keyspace_exists(host: str) -> bool:
    if Cluster is None:
        return False
    hp = (host.strip() or CASSANDRA_HOST).strip() or CASSANDRA_HOST
    c = _cassandra_cluster_ephemeral((hp,), "", "")
    if c is None:
        return False
    try:
        s = c.connect()
        rows = s.execute("SELECT keyspace_name FROM system_schema.keyspaces;")
        ks = [r.keyspace_name for r in rows]
        return KEYSPACE in ks
    except Exception:
        return False
    finally:
        try:
            c.shutdown()
        except Exception:
            pass


def _aplicar_esquema_cassandra_if_needed() -> Tuple[bool, str]:
    """
    Aplica `cassandra/esquema_smart_grid.cql` si el keyspace `smart_grid` no existe.

    Nota: no usamos `cqlsh` porque no siempre está instalado; se ejecuta vía cassandra-driver.
    """
    if Cluster is None:
        return False, "cassandra-driver no instalado en el entorno de Streamlit."

    if _cassandra_keyspace_exists(CASSANDRA_HOST):
        return True, "Esquema ya aplicado (keyspace existe)."

    cql_path = BASE / "cassandra" / "esquema_smart_grid.cql"
    if not cql_path.exists():
        return False, f"No encontré el esquema: {cql_path}"

    raw = cql_path.read_text(encoding="utf-8")
    stmts: List[str] = []
    for line in raw.splitlines():
        s = line.strip()
        if not s or s.startswith("--"):
            continue
        stmts.append(s)

    # El esquema no debería tener ; dentro de strings, así que lo partimos simple.
    joined = "\n".join(stmts)
    pieces = [p.strip() for p in joined.split(";") if p.strip()]

    auth_user = ""
    auth_pass = ""
    session_cluster = _cassandra_cluster_ephemeral((CASSANDRA_HOST,), auth_user, auth_pass)
    if session_cluster is None:
        return False, "No se pudo crear cluster Cassandra."
    try:
        session = session_cluster.connect()
        ok = 0
        for stmt in pieces:
            session.execute(stmt)
            ok += 1
        # Invalidar cache del dashboard: el Cluster cacheado podía estar cerrado o sin el keyspace nuevo.
        _cluster_cassandra.clear()
        return True, f"Esquema aplicado correctamente ({ok} sentencias)."
    except Exception as e:
        return False, f"Error aplicando esquema: {e}"
    finally:
        try:
            session_cluster.shutdown()
        except Exception:
            pass


def _kafka_topics_exist(bootstrap: str) -> Tuple[bool, List[str]]:
    try:
        kafka_topics = Path(KAFKA_HOME) / "bin" / "kafka-topics.sh"
        if not kafka_topics.exists():
            return False, []
        r = _safe_run(
            [
                str(kafka_topics),
                "--list",
                "--bootstrap-server",
                bootstrap,
            ],
            timeout_s=20,
        )
        if r["rc"] != 0:
            return False, []
        topics = [t.strip() for t in r["stdout"].splitlines() if t.strip()]
        need = {TOPIC_RAW, TOPIC_WEATHER_RAW}
        return need.issubset(set(topics)), topics  # gps_raw opcional
    except Exception:
        return False, []


def _aplicar_topics_kafka(bootstrap: str) -> Tuple[bool, str]:
    try:
        kafka_topics = Path(KAFKA_HOME) / "bin" / "kafka-topics.sh"
        if not kafka_topics.exists():
            return False, f"Kafka no instalado: no existe {kafka_topics}"
        cmds = [
            [
                str(kafka_topics),
                "--create",
                "--topic",
                TOPIC_RAW,
                "--bootstrap-server",
                bootstrap,
                "--partitions",
                "2",
                "--replication-factor",
                "1",
            ],
            [
                str(kafka_topics),
                "--create",
                "--topic",
                TOPIC_WEATHER_RAW,
                "--bootstrap-server",
                bootstrap,
                "--partitions",
                "2",
                "--replication-factor",
                "1",
            ],
            [
                str(kafka_topics),
                "--create",
                "--topic",
                TOPIC_GPS_RAW,
                "--bootstrap-server",
                bootstrap,
                "--partitions",
                "2",
                "--replication-factor",
                "1",
            ],
        ]
        out_all = []
        for c in cmds:
            r = _safe_run(c, timeout_s=20)
            out_all.append((c[-3], r["rc"], (r["stdout"] + r["stderr"])[:800]))
        ok_topics, _ = _kafka_topics_exist(bootstrap)
        if ok_topics:
            return True, "Topics `energy_raw`, `weather_raw` y `gps_raw` listos."
        return False, f"Topics no están listos. Salida: {out_all}"
    except Exception as e:
        return False, f"Error creando topics: {e}"


def _start_hdfs() -> Tuple[bool, str]:
    # Si HDFS/NameNode ya responden, considerarlo OK (evita "process is running, stop it first")
    try:
        rr = _safe_run(["hdfs", "dfs", "-ls", "/"], timeout_s=10)
        if rr["rc"] == 0:
            return True, "HDFS ya estaba activo (NameNode responde)."
    except Exception:
        pass
    try:
        r = _safe_run(["/opt/hadoop/sbin/start-all.sh"], timeout_s=120)
        out = (r["stdout"] + "\n" + r["stderr"]).strip()[-1500:]
        if r["rc"] == 0:
            return True, out
        # Si falló pero HDFS responde (procesos ya corrían), considerarlo OK
        rr = _safe_run(["hdfs", "dfs", "-ls", "/"], timeout_s=5)
        if rr["rc"] == 0:
            return True, "HDFS/YARN ya estaban activos. " + out
        return False, out
    except Exception as e:
        return False, f"Error arrancando HDFS: {e}"


def _start_cassandra() -> Tuple[bool, str]:
    # Si el puerto 9042 ya está abierto, asumimos que Cassandra está levantada.
    if _port_open("127.0.0.1", 9042):
        return True, "Cassandra ya responde en 9042."
    cassandra_bin = BASE / "cassandra" / "bin" / "cassandra"
    if not cassandra_bin.exists():
        return False, f"No encuentro cassandra bin: {cassandra_bin}"

    log_path = Path("/tmp/smart_grid_streamlit_cassandra.log")
    try:
        # Arrancar en background, redirigiendo logs a un fichero.
        with log_path.open("a", encoding="utf-8") as f:
            p = subprocess.Popen(
                [str(cassandra_bin)],
                stdout=f,
                stderr=subprocess.STDOUT,
                cwd=str(BASE),
            )
        return True, f"Cassandra arrancada (pid={p.pid}). Log: {log_path}"
    except Exception as e:
        return False, f"Error arrancando Cassandra: {e}"


def _start_kafka() -> Tuple[bool, str]:
    if _port_open("127.0.0.1", 9092):
        return True, "Kafka ya responde en 9092."
    kafka_home = Path(KAFKA_HOME)
    start_script = kafka_home / "bin" / "kafka-server-start.sh"
    server_props = kafka_home / "config" / "kraft" / "server.properties"
    if not server_props.exists():
        server_props = kafka_home / "config" / "server.properties"
    if not start_script.exists() or not server_props.exists():
        return False, f"Kafka no instalado. Ejecuta: ./scripts/instalar_kafka_local.sh"
    log_path = Path("/tmp/smart_grid_streamlit_kafka.log")
    try:
        with log_path.open("a", encoding="utf-8") as f:
            p = subprocess.Popen(
                [str(start_script), str(server_props)],
                stdout=f,
                stderr=subprocess.STDOUT,
                cwd=str(kafka_home),
            )
        return True, f"Kafka arrancado (pid={p.pid}). Log: {log_path}"
    except Exception as e:
        return False, f"Error arrancando Kafka: {e}"


def _stop_hdfs() -> Tuple[bool, str]:
    try:
        r = _safe_run(["/opt/hadoop/sbin/stop-all.sh"], timeout_s=120)
        return (r["rc"] == 0), (r["stdout"][-1500:] + "\n" + r["stderr"][-1500:]).strip()
    except Exception as e:
        return False, f"Error parando HDFS: {e}"


def _stop_kafka() -> Tuple[bool, str]:
    # Mejor-effort para no romper el resto del sistema.
    try:
        r = subprocess.run(
            ["pkill", "-f", "kafka-server-start.sh"],
            capture_output=True,
            text=True,
        )
        return True, f"pkill kafka-server-start.sh rc={r.returncode} (ok si ya estaba parado)."
    except Exception as e:
        return False, f"Error parando Kafka: {e}"


def _stop_cassandra() -> Tuple[bool, str]:
    try:
        r = subprocess.run(
            ["pkill", "-f", "cassandra/bin/cassandra"],
            capture_output=True,
            text=True,
        )
        return True, f"pkill Cassandra rc={r.returncode} (ok si ya estaba parado)."
    except Exception as e:
        return False, f"Error parando Cassandra: {e}"


def _hive_catalog_db_in_show_databases_output(text: str, db: str) -> bool:
    """
    Comprueba si el nombre de base aparece en la salida de SHOW DATABASES
    (insensible a mayúsculas, tolera formato tabla con | y backticks).
    """
    if not text or not db:
        return False
    blob = text.lower().replace("`", "")
    d = db.strip().lower()
    if d in blob:
        return True
    for line in text.splitlines():
        if "|" not in line:
            continue
        cells = [c.strip().lower().rstrip("`") for c in line.split("|")]
        if d in cells:
            return True
    return False


def _comprobar_servicios_base() -> Dict[str, Any]:
    hdfs_ok = False
    try:
        # Confirma que HDFS CLI responde.
        rr = _safe_run(["hdfs", "dfs", "-ls", "/"], timeout_s=15)
        hdfs_ok = rr["rc"] == 0
    except Exception:
        hdfs_ok = False

    kafka_ok = _port_open("127.0.0.1", 9092)
    cassandra_ok = _port_open("127.0.0.1", 9042)

    ks_ok = _cassandra_keyspace_exists(CASSANDRA_HOST)
    topics_ok, topics = _kafka_topics_exist(KAFKA_BOOTSTRAP)

    hive_cli_available = bool(_hive_sql_cli_paths())
    hive_catalog_ok = False
    hive_catalog_hint = ""
    if hive_cli_available:
        try:
            _hrc, _hout = _ejecutar_consulta_hive("SHOW DATABASES;", catalog_probe=True)
            if _hrc == 0:
                hive_catalog_ok = _hive_catalog_db_in_show_databases_output(_hout or "", HIVE_DB)
                if not hive_catalog_ok:
                    hive_catalog_hint = (
                        f"No aparece la base `{HIVE_DB}` en SHOW DATABASES. "
                        "Crea el esquema: `hive -f setup_hive.hql` o `spark-sql -f setup_hive.hql` (desde la raíz del repo)."
                    )
            else:
                tail = ((_hout or "")[:400]).replace("\n", " ")
                _hout_l = (_hout or "").lower()
                if "xslan" in _hout_l or "incompatible format" in _hout_l:
                    hive_catalog_hint = (
                        "Metastore Derby incompatible (formato antiguo). Ejecuta en terminal: "
                        "`./scripts/fix_hive_metastore_derby_incompatible.sh` y luego Fase 0 → Arrancar o "
                        "`spark-sql -f setup_hive.hql`."
                    )
                elif "timeout" in _hout_l:
                    hive_catalog_hint = (
                        "SHOW DATABASES sigue excediendo tiempo tras reintento (metastore/Spark/JVM fríos o carga alta). "
                        "El catálogo puede estar bien. Opciones: esperar y recargar el panel; en terminal: "
                        "`spark-sql -e \"SHOW DATABASES;\" 2>&1` o `hive -e \"SHOW DATABASES;\" 2>&1` "
                        "(la primera ejecución puede tardar 1–2 min y parte de la salida va a stderr). "
                        "Subir límites: `HIVE_CATALOG_PROBE_HIVE_TIMEOUT_SEC` / "
                        "`HIVE_CATALOG_PROBE_SPARK_TIMEOUT_SEC` (p. ej. 180 y 360)."
                    )
                else:
                    hive_catalog_hint = f"SHOW DATABASES devolvió rc={_hrc}. {tail}"
        except Exception as ex:
            hive_catalog_hint = str(ex)[:300]

    out: Dict[str, Any] = {
        "hdfs_ok": hdfs_ok,
        "kafka_ok": kafka_ok,
        "cassandra_ok": cassandra_ok,
        "keyspace_ok": ks_ok,
        "topics_ok": topics_ok,
        "topics": topics,
        "hive_cli_available": hive_cli_available,
        "hive_catalog_ok": hive_catalog_ok,
    }
    if hive_catalog_hint:
        out["hive_catalog_hint"] = hive_catalog_hint
    return out


def _fase0_arrancar_servicios() -> Dict[str, Any]:
    """
    Fase 0 de demostración: levantar los servicios base y dejar el entorno listo:
    HDFS -> Kafka -> Cassandra -> Topics -> Keyspace/Esquema.
    """
    out: Dict[str, Any] = {"orden": [], "resultados": {}}

    # 1) HDFS
    out["orden"].append("1) HDFS (start-all.sh)")
    ok, msg = _start_hdfs()
    out["resultados"]["hdfs_start"] = {"ok": ok, "msg": msg}

    # 2) Kafka
    out["orden"].append("2) Kafka (kafka-server-start.sh)")
    ok, msg = _start_kafka()
    out["resultados"]["kafka_start"] = {"ok": ok, "msg": msg}

    # 3) Cassandra
    out["orden"].append("3) Cassandra (cassandra/bin/cassandra)")
    ok, msg = _start_cassandra()
    out["resultados"]["cassandra_start"] = {"ok": ok, "msg": msg}

    # Pequeña espera para que los puertos se abran.
    time.sleep(2)

    # 4) Topics Kafka
    out["orden"].append("4) Topics (energy_raw, weather_raw)")
    ok, msg = _aplicar_topics_kafka(KAFKA_BOOTSTRAP)
    out["resultados"]["topics"] = {"ok": ok, "msg": msg}

    # 5) Esquema Cassandra (keyspace smart_grid)
    out["orden"].append("5) Esquema Cassandra (smart_grid)")
    ok, msg = _aplicar_esquema_cassandra_if_needed()
    out["resultados"]["esquema_cassandra"] = {"ok": ok, "msg": msg}

    # 6) Esquema Hive (setup_hive.hql → base smart_grid_analytics)
    out["orden"].append("6) Esquema Hive (setup_hive.hql)")
    ok, msg = _aplicar_esquema_hive_if_needed()
    out["resultados"]["esquema_hive"] = {"ok": ok, "msg": msg}

    out["check_final"] = _comprobar_servicios_base()
    return out


def _start_hdfs_yarn() -> Tuple[bool, str]:
    """Arranca HDFS (NameNode 9870) y YARN (ResourceManager 8088, Job History 19888)."""
    try:
        # start-all.sh incluye DFS + YARN; si no existe, intentar por separado
        hadoop_sbin = Path(os.environ.get("HADOOP_HOME", "/opt/hadoop")) / "sbin"
        start_all = hadoop_sbin / "start-all.sh"
        if start_all.exists():
            r = _safe_run([str(start_all)], timeout_s=120)
            return (r["rc"] == 0), (r["stdout"][-1200:] + "\n" + r["stderr"][-1200:]).strip()
        start_dfs = hadoop_sbin / "start-dfs.sh"
        start_yarn = hadoop_sbin / "start-yarn.sh"
        out = []
        if start_dfs.exists():
            r1 = _safe_run([str(start_dfs)], timeout_s=90)
            out.append(f"start-dfs: rc={r1['rc']}")
        if start_yarn.exists():
            r2 = _safe_run([str(start_yarn)], timeout_s=90)
            out.append(f"start-yarn: rc={r2['rc']}")
        return True, "\n".join(out) if out else "No se encontró start-all.sh ni start-dfs/start-yarn."
    except Exception as e:
        return False, str(e)


def _start_dfs_only() -> Tuple[bool, str]:
    """
    Solo HDFS vía start-dfs.sh (NameNode RPC ~9000, Web UI 9870).
    No arranca YARN: útil para Hive CLI / warehouse sin ResourceManager.
    Equivale a: $HADOOP_HOME/sbin/start-dfs.sh
    """
    try:
        rr = _safe_run(["hdfs", "dfs", "-ls", "/"], timeout_s=10)
        if rr["rc"] == 0:
            return True, "HDFS ya estaba activo (hdfs dfs -ls / OK)."
    except Exception:
        pass
    hadoop_sbin = Path(os.environ.get("HADOOP_HOME", "/opt/hadoop")) / "sbin"
    start_dfs = hadoop_sbin / "start-dfs.sh"
    if not start_dfs.exists():
        return False, f"No se encuentra {start_dfs}. Define HADOOP_HOME=/opt/hadoop (o tu ruta)."
    r = _safe_run([str(start_dfs)], timeout_s=120)
    out = (r["stdout"] + "\n" + r["stderr"]).strip()[-2500:]
    if r["rc"] == 0:
        return True, out or "start-dfs.sh terminó (rc=0)."
    try:
        rr2 = _safe_run(["hdfs", "dfs", "-ls", "/"], timeout_s=15)
        if rr2["rc"] == 0:
            return True, f"HDFS responde aunque start-dfs devolvió rc={r['rc']}.\n{out}"
    except Exception:
        pass
    return False, out or f"start-dfs.sh falló (rc={r['rc']}). Revisa logs en $HADOOP_HOME/logs."


def _start_job_history_server() -> Tuple[bool, str]:
    """Arranca el MapReduce Job History Server (puerto 19888)."""
    if _port_open("127.0.0.1", 19888):
        return True, "Job History Server ya estaba activo en 19888."
    try:
        # Java 17+ requiere --add-opens para Guice/cglib en Job History Server
        hadoop_home = os.environ.get("HADOOP_HOME", "/opt/hadoop")
        env = os.environ.copy()
        jvm_opts = (
            "--add-opens java.base/java.lang=ALL-UNNAMED "
            "--add-opens java.base/java.util=ALL-UNNAMED "
            "--add-opens java.base/java.lang.reflect=ALL-UNNAMED"
        )
        env["HADOOP_OPTS"] = f"{env.get('HADOOP_OPTS', '')} {jvm_opts}".strip()
        mapred = Path(hadoop_home) / "bin" / "mapred"
        if mapred.exists():
            r = subprocess.run(
                [str(mapred), "--daemon", "start", "historyserver"],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(BASE),
            )
            return (r.returncode == 0), (r.stdout + "\n" + r.stderr).strip()
        daemon = Path(hadoop_home) / "sbin" / "mr-jobhistory-daemon.sh"
        if daemon.exists():
            r = subprocess.run(
                [str(daemon), "start", "historyserver"],
                env=env,
                capture_output=True,
                text=True,
                timeout=30,
                cwd=str(BASE),
            )
            return (r.returncode == 0), (r.stdout + "\n" + r.stderr).strip()
        return False, "No se encontró mapred ni mr-jobhistory-daemon.sh"
    except Exception as e:
        return False, str(e)


def _start_spark_history_server() -> Tuple[bool, str]:
    """Arranca el Spark History Server (puerto 18080)."""
    if _port_open("127.0.0.1", 18080):
        return True, "Spark History Server ya estaba activo en 18080."
    try:
        spark_home = Path(os.environ.get("SPARK_HOME", "/opt/spark"))
        script = spark_home / "sbin" / "start-history-server.sh"
        if script.exists():
            r = _safe_run([str(script)], timeout_s=30)
            return (r["rc"] == 0), (r["stdout"] + "\n" + r["stderr"]).strip()
        return False, "No se encontró SPARK_HOME/sbin/start-history-server.sh"
    except Exception as e:
        return False, str(e)


def _restart_yarn() -> Tuple[bool, str]:
    """Reinicia YARN para aplicar cambios en yarn-site.xml (p. ej. webapp en 0.0.0.0)."""
    try:
        hadoop_sbin = Path(os.environ.get("HADOOP_HOME", "/opt/hadoop")) / "sbin"
        stop = hadoop_sbin / "stop-yarn.sh"
        start = hadoop_sbin / "start-yarn.sh"
        if not stop.exists() or not start.exists():
            return False, "No se encontró stop-yarn.sh o start-yarn.sh"
        r1 = _safe_run([str(stop)], timeout_s=60)
        time.sleep(3)
        r2 = _safe_run([str(start)], timeout_s=90)
        return (r2["rc"] == 0), f"stop: rc={r1['rc']}\nstart: rc={r2['rc']}\n{r2['stdout'][-500:]}\n{r2['stderr'][-500:]}"
    except Exception as e:
        return False, str(e)


def _start_kafka_docker() -> Tuple[bool, str]:
    """Arranca Kafka + Kafdrop en Docker (puertos 9092 y 9090)."""
    if _port_open("127.0.0.1", 9092):
        return True, "Kafka ya responde en 9092."
    compose_path = BASE / "docker" / "docker-compose-kafka.yml"
    if not compose_path.exists():
        return False, f"No existe {compose_path}"
    try:
        # Probar docker compose (v2) o docker-compose (v1)
        for cmd in [["docker", "compose", "-f", str(compose_path), "up", "-d"],
                    ["docker-compose", "-f", str(compose_path), "up", "-d"]]:
            r = _safe_run(cmd, timeout_s=90)
            out = (r["stdout"] + "\n" + r["stderr"]).strip()
            if r["rc"] == 0:
                time.sleep(12)
                for topic in [TOPIC_RAW, TOPIC_WEATHER_RAW]:
                    _safe_run(
                        ["docker", "exec", "smartgrid-kafka",
                         "/opt/bitnami/kafka/bin/kafka-topics.sh",
                         "--create", "--topic", topic, "--bootstrap-server", "kafka:9092",
                         "--partitions", "2", "--replication-factor", "1"],
                        timeout_s=15,
                    )
                return True, "Kafka + Kafdrop arrancando. Topics energy_raw y weather_raw creados."
            if "not found" in out.lower() or "no such" in out.lower():
                continue
            break
        if "Cannot connect to the Docker daemon" in out or "permission denied" in out.lower():
            return False, "Docker no está corriendo o no tienes permisos. Ejecuta: ./docker/instalar_docker_kafka.sh"
        return False, out[-800:]
    except FileNotFoundError:
        return False, "Docker no instalado. Ejecuta: ./docker/instalar_docker_kafka.sh"
    except Exception as e:
        return False, str(e)


def _start_kafdrop(bootstrap: str = "") -> Tuple[bool, str]:
    """Arranca Kafdrop (Kafka UI) en puerto 9090 vía Docker."""
    if _port_open("127.0.0.1", 9090):
        return True, "Kafdrop ya está activo en 9090."
    bs = (bootstrap or KAFKA_BOOTSTRAP).split(",")[0].strip()
    try:
        r = _safe_run(
            [
                "docker", "run", "-d",
                "--name", "kafdrop-smartgrid",
                "-p", "9090:9090",
                "-e", f"KAFKA_BROKERCONNECT={bs}",
                "obsidiandynamics/kafdrop:latest",
            ],
            timeout_s=60,
        )
        if r["rc"] == 0:
            return True, "Kafdrop arrancado en 9090. Espera unos segundos."
        # Si el contenedor ya existe, intentar start
        r2 = _safe_run(["docker", "start", "kafdrop-smartgrid"], timeout_s=15)
        if r2["rc"] == 0:
            return True, "Contenedor kafdrop-smartgrid iniciado."
        return False, (r["stderr"] + r["stdout"]).strip()[-800:]
    except FileNotFoundError:
        return False, "Docker no está instalado o no está en el PATH."
    except Exception as e:
        return False, str(e)


def _get_default_ui_host() -> str:
    """Obtiene el host por defecto para los enlaces (IP o hostname del servidor)."""
    env_host = os.environ.get("HADOOP_UI_HOST")
    if env_host:
        return env_host
    try:
        # Preferir la IP de la interfaz principal (no loopback)
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        pass
    try:
        return socket.gethostname()
    except Exception:
        return "localhost"


def _ejecutar_consulta_hdfs(cmd: str) -> Tuple[int, str]:
    """Ejecuta un comando hdfs dfs y devuelve (rc, salida)."""
    parts = cmd.strip().split()
    if parts[0] == "hdfs":
        parts = parts[1:]
    if not parts or parts[0] != "dfs":
        parts = ["dfs"] + parts
    r = _safe_run(["hdfs"] + parts, timeout_s=30)
    return r["rc"], (r["stdout"] + "\n" + r["stderr"]).strip()


def _ejecutar_consulta_hive(
    query: str,
    quick_check: bool = False,
    catalog_probe: bool = False,
) -> Tuple[int, str]:
    """
    Ejecuta SQL Hive vía spark-sql (mismo catálogo que Spark) o hive -e.

    quick_check=True: prueba **hive** antes que **spark-sql** y timeouts moderados
    (spark-sql puede tardar mucho en arrancar la JVM; evita colgar Fase 0 / comprobaciones).

    catalog_probe=True: para SHOW DATABASES en monitorización — **hive primero**, timeouts largos
    (env `HIVE_CATALOG_PROBE_HIVE_TIMEOUT_SEC` / `HIVE_CATALOG_PROBE_SPARK_TIMEOUT_SEC`) y **un reintento**
    si solo hubo timeout (metastore/Spark frío).
    """
    clean = query.strip().rstrip(";")
    env = _hive_cli_env()
    spark_paths: List[str] = []
    hive_paths: List[str] = []
    if env.get("SPARK_HOME"):
        spark_paths.append(str(Path(env["SPARK_HOME"]) / "bin" / "spark-sql"))
    if Path(SPARK_HOME).joinpath("bin/spark-sql").exists():
        p = str(Path(SPARK_HOME) / "bin" / "spark-sql")
        if p not in spark_paths:
            spark_paths.append(p)
    if env.get("HIVE_HOME"):
        hive_paths.append(str(Path(env["HIVE_HOME"]) / "bin" / "hive"))
    if Path(HIVE_HOME).joinpath("bin/hive").exists():
        p = str(Path(HIVE_HOME) / "bin" / "hive")
        if p not in hive_paths:
            hive_paths.append(p)
    for extra in ("/opt/spark/bin/spark-sql", "/usr/bin/spark-sql"):
        if extra not in spark_paths and Path(extra).exists():
            spark_paths.append(extra)

    candidates: List[str] = []
    hive_first = quick_check or catalog_probe
    if hive_first:
        candidates.extend(hive_paths)
        candidates.extend(spark_paths)
    else:
        candidates.extend(spark_paths)
        candidates.extend(hive_paths)
    candidates = list(dict.fromkeys(candidates))

    def _timeouts_for_exe(name: str) -> int:
        if catalog_probe:
            try:
                th = int(os.environ.get("HIVE_CATALOG_PROBE_HIVE_TIMEOUT_SEC", "120"))
                ts = int(os.environ.get("HIVE_CATALOG_PROBE_SPARK_TIMEOUT_SEC", "240"))
            except ValueError:
                th, ts = 120, 240
            return th if name == "hive" else ts
        if quick_check:
            return 90 if name == "hive" else 180
        return 90 if name == "hive" else 180

    def _run_candidates() -> Tuple[int, str]:
        last_err = ""
        seen = set()
        for exe in candidates:
            if not exe or exe in seen or not Path(exe).exists():
                continue
            seen.add(exe)
            name = Path(exe).name
            timeout_s = _timeouts_for_exe(name)
            r = _safe_run([exe, "-e", clean], timeout_s=timeout_s, env=env)
            out = (r["stdout"] + "\n" + r["stderr"]).strip()
            if r["rc"] == 0:
                return 0, out
            if r["rc"] == 124:
                last_err = (last_err + "\n" if last_err else "") + f"{exe}: timeout ({timeout_s}s)"
                continue
            last_err = out
        if seen:
            return -1, last_err or "Error ejecutando consulta Hive."
        return (
            -1,
            "No se encontró spark-sql ni hive. Instala Hive 4.x: ./scripts/instalar_hive_java21.sh "
            "y/o define SPARK_HOME=/opt/spark. Usa Java 21 con Hive 4.2.",
        )

    rc, out = _run_candidates()
    if rc == 0:
        return 0, out
    if catalog_probe and "timeout" in (out or "").lower():
        time.sleep(4)
        rc, out = _run_candidates()
    return rc, out


def _hive_sql_cli_paths() -> List[str]:
    """Rutas a hive / spark-sql (hive primero: suele arrancar antes que la JVM pesada de Spark)."""
    env = _hive_cli_env()
    out: List[str] = []
    for hive_home in filter(None, [env.get("HIVE_HOME"), HIVE_HOME]):
        p = Path(hive_home) / "bin" / "hive"
        if p.exists():
            out.append(str(p))
    for spark_home in filter(None, [env.get("SPARK_HOME"), SPARK_HOME]):
        p = Path(spark_home) / "bin" / "spark-sql"
        if p.exists():
            out.append(str(p))
    for extra in ("/opt/spark/bin/spark-sql", "/usr/bin/spark-sql"):
        if extra not in out and Path(extra).exists():
            out.append(extra)
    return list(dict.fromkeys(out))


def _aplicar_esquema_hive_if_needed() -> Tuple[bool, str]:
    """
    Ejecuta setup_hive.hql con spark-sql -f o hive -f si HDFS responde y la base aún no existe.
    Si no hay CLI Hive/Spark, no bloquea Fase 0 (mensaje informativo).
    """
    hql = (BASE / "setup_hive.hql").resolve()
    if not hql.exists():
        return False, f"No se encuentra {hql}"
    try:
        rr = _safe_run(["hdfs", "dfs", "-ls", "/"], timeout_s=12)
        if rr["rc"] != 0:
            return True, "Hive omitido: HDFS no responde (arranca HDFS primero)."
    except Exception:
        return True, "Hive omitido: HDFS no responde."
    rc_chk, out_chk = _ejecutar_consulta_hive("SHOW DATABASES;", catalog_probe=True)
    if rc_chk == 0 and HIVE_DB in (out_chk or ""):
        return True, "Esquema Hive ya presente (base listada)."
    if rc_chk != 0 and "timeout" in (out_chk or "").lower():
        return (
            True,
            "Hive omitido: SHOW DATABASES excedió tiempo (JVM/Spark/metastore lento o no listo). "
            "Cuando arranque Hive/Spark, ejecuta manualmente: spark-sql o hive -f setup_hive.hql",
        )
    clients = _hive_sql_cli_paths()
    if not clients:
        return True, (
            "Hive omitido: no hay spark-sql/hive instalado. "
            "Instala: ./scripts/instalar_hive_java21.sh y/o define SPARK_HOME."
        )
    env = _hive_cli_env()
    warehouse = f"{HDFS_DEFAULT_FS.rstrip('/')}/user/hive/warehouse"
    last_err = ""
    for exe in clients:
        cmd: List[str]
        if Path(exe).name == "spark-sql":
            cmd = [exe, "--conf", f"spark.sql.warehouse.dir={warehouse}", "-f", str(hql)]
        else:
            cmd = [exe, "-f", str(hql)]
        r = _safe_run(cmd, timeout_s=240, env=env)
        out = (r["stdout"] + "\n" + r["stderr"]).strip()
        if r["rc"] == 0:
            return True, f"Esquema Hive aplicado con {Path(exe).name}"
        last_err = out[-2500:] or ""
    return False, last_err or "No se pudo aplicar setup_hive.hql"


def _parse_hive_spark_sql_cli_output(text: str) -> Optional[List[Dict[str, Any]]]:
    """Convierte salida tipo tabla de spark-sql/hive CLI en lista de dicts (si el formato es reconocible)."""
    rows: List[List[str]] = []
    for line in text.splitlines():
        if "|" not in line:
            continue
        s = line.rstrip()
        if s.startswith("SLF4J") or "Time taken" in s or "WARN" in s[:20]:
            continue
        if re.match(r"^[\s|+\-]+$", s):
            continue
        inner = [p.strip() for p in s.split("|")[1:-1]]
        if not inner:
            continue
        if all(c in "+- " for c in "".join(inner)):
            continue
        rows.append(inner)
    if len(rows) < 2:
        return None
    header = rows[0]
    if not header or len(header) < 1:
        return None
    out: List[Dict[str, Any]] = []
    for r in rows[1:]:
        if len(r) != len(header):
            continue
        out.append({header[i]: r[i] for i in range(len(header))})
    return out if out else None


def _row_cassandra_to_dict(r) -> Dict[str, Any]:
    """Convierte una fila Cassandra a dict."""
    if hasattr(r, "_asdict"):
        return r._asdict()
    if hasattr(r, "_fields"):
        return {f: getattr(r, f) for f in r._fields}
    return dict(zip(range(len(r)), r))


def _ejecutar_consulta_cassandra_cql(cql: str, host: str = "") -> Tuple[bool, Any]:
    """Ejecuta CQL vía cassandra-driver. Devuelve (ok, rows_list o error_str)."""
    if Cluster is None:
        return False, "cassandra-driver no instalado."
    try:
        c = _cluster_cassandra((host.strip() or CASSANDRA_HOST,), "", "")
        if not c:
            return False, "No se pudo conectar al cluster."
        s = c.connect(KEYSPACE)
        rows = list(s.execute(cql.strip().rstrip(";")))
        return True, [_row_cassandra_to_dict(r) for r in rows]
    except Exception as e:
        return False, str(e)


def _yarn_application_list() -> Tuple[int, str]:
    """Lista aplicaciones YARN (jobs)."""
    r = _safe_run(["yarn", "application", "-list"], timeout_s=15)
    return r["rc"], (r["stdout"] + "\n" + r["stderr"]).strip()


def _ejecutar_kafka_consumer_sample(topic: str, max_messages: int = 5) -> Tuple[bool, Any]:
    """Obtiene una muestra de mensajes de un topic Kafka."""
    try:
        from kafka import KafkaConsumer
        import json as _json
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=3000,
        )
        msgs = []
        for i, m in enumerate(consumer):
            if i >= max_messages:
                break
            try:
                msgs.append({"offset": m.offset, "partition": m.partition, "value": m.value.decode("utf-8", errors="replace")[:500]})
            except Exception:
                msgs.append({"offset": m.offset, "partition": m.partition, "value": "(binario)"})
        return True, msgs
    except Exception as e:
        return False, str(e)


def _fase0_parar_servicios() -> Dict[str, Any]:
    out = {"resultados": {}}
    ok, msg = _stop_hdfs()
    out["resultados"]["hdfs_stop"] = {"ok": ok, "msg": msg}
    ok, msg = _stop_kafka()
    out["resultados"]["kafka_stop"] = {"ok": ok, "msg": msg}
    ok, msg = _stop_cassandra()
    out["resultados"]["cassandra_stop"] = {"ok": ok, "msg": msg}
    out["check_final"] = _comprobar_servicios_base()
    return out


def _verificar_kafka_mensajes(bootstrap: str) -> Dict[str, Any]:
    """
    Verifica si hay al menos 1 mensaje reciente en `energy_raw` y `weather_raw`.
    """
    try:
        from kafka import KafkaConsumer
        import json as _json

        consumer = KafkaConsumer(
            TOPIC_RAW,
            TOPIC_WEATHER_RAW,
            bootstrap_servers=bootstrap,
            auto_offset_reset="latest",
            enable_auto_commit=False,
            consumer_timeout_ms=2500,
            value_deserializer=lambda v: _json.loads(v.decode("utf-8")),
        )
        got = {TOPIC_RAW: False, TOPIC_WEATHER_RAW: False}
        t0 = time.time()
        for msg in consumer:
            got[msg.topic] = True
            # Nos basta con confirmar que llegaron.
            if got[TOPIC_RAW] and got[TOPIC_WEATHER_RAW]:
                break
            if time.time() - t0 > 3.0:
                break
        return {"ok": got[TOPIC_RAW] and got[TOPIC_WEATHER_RAW], "got": got}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _verificar_hdfs_backup() -> Dict[str, Any]:
    """
    Verifica que exista un backup reciente de energy (producer escribe energy a HDFS).
    """
    try:
        # Listado simple; si hay archivos, asumimos que producer corrió.
        r = _safe_run(["hdfs", "dfs", "-ls", HDFS_BACKUP_PATH], timeout_s=20)
        if r["rc"] != 0:
            return {"ok": False, "error": r["stderr"][-800:]}
        lines = [ln.strip() for ln in r["stdout"].splitlines() if ln.strip()]
        energy_files = [ln for ln in lines if "/energy_" in ln and ln.endswith(".json")]
        weather_files = [ln for ln in lines if "/weather_" in ln and ln.endswith(".json")]
        # Producer actual escribe energy_* (no weather_* en HDFS).
        return {
            "ok": len(energy_files) > 0,
            "energy_files": energy_files[-5:],
            "weather_files": weather_files[-5:],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _fase1_ejecutar_producer() -> Tuple[int, str]:
    r = _safe_run([sys.executable, str(BASE / "producer.py")], timeout_s=180)
    return r["rc"], (r["stdout"] + "\n" + r["stderr"]).strip()


def _fase2_ejecutar_spark() -> Tuple[int, str]:
    r = _safe_run(
        [sys.executable, str(BASE / "procesamiento" / "procesamiento_grafos.py")],
        timeout_s=600,
    )
    return r["rc"], (r["stdout"] + "\n" + r["stderr"]).strip()


def _fase2_verificar_cassandra() -> Dict[str, Any]:
    if Cluster is None:
        return {"ok": False, "error": "cassandra-driver no instalado."}
    try:
        s = obtener_session_cassandra(CASSANDRA_HOST)
        if not s:
            return {"ok": False, "error": "No se pudo conectar a Cassandra."}
        counts: Dict[str, int] = {}
        for table in ["subestaciones_estado", "lineas_estado", "pagerank_subestaciones", "puntos_fallo_unicos"]:
            cnt = s.execute(f"SELECT COUNT(*) AS cnt FROM {KEYSPACE}.{table};").one().cnt
            counts[table] = int(cnt or 0)
        # No llamar s.shutdown(): puede dejar el Cluster cacheado (_cluster_cassandra) en «already shut down».
        ok = counts.get("subestaciones_estado", 0) > 0 and counts.get("puntos_fallo_unicos", 0) > 0
        return {"ok": ok, "counts": counts}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _nifi_home() -> Path:
    p = Path(NIFI_HOME)
    return p if p.exists() else BASE / "nifi-2.6.0"


def _nifi_flow_path() -> Path:
    return _nifi_home() / "conf" / "flow.json.gz"


def _nifi_ports_default() -> List[int]:
    # En `conf/nifi.properties` el UI está típicamente en HTTPS:8443.
    # (Nos quedamos con 8443 como verificación principal.)
    return [8443, 8080]


def _nifi_is_running() -> Dict[str, Any]:
    ports = _nifi_ports_default()
    port_ok = {p: _port_open("127.0.0.1", p) for p in ports}
    running = any(port_ok.values())
    return {"running": running, "ports": port_ok}


def _nifi_flow_processors_activos() -> List[str]:
    """
    Devuelve los nombres de procesadores actuales del root process group.

    Nota: en este repo el `conf/flow.json.gz` viene vacío por defecto, así que
    para reflejar lo que realmente se ha creado/arrancado usamos la API REST.
    """
    pg_id = _nifi_get_root_group_id()
    if not pg_id:
        return []
    try:
        procs = _nifi_list_processors(pg_id)
        return sorted(procs.keys())
    except Exception:
        return []


def _nifi_start() -> Tuple[bool, str]:
    nifi_sh = _nifi_home() / "bin" / "nifi.sh"
    if not nifi_sh.exists():
        return False, f"No encuentro nifi.sh en: {nifi_sh}"

    try:
        r = _safe_run([str(nifi_sh), "start"], timeout_s=240)
        # No siempre la apertura de puertos ocurre en el mismo segundo.
        time.sleep(2)
        chk = _nifi_is_running()
        return bool(r["rc"] == 0) and chk["running"], (r["stdout"] + "\n" + r["stderr"]).strip()
    except Exception as e:
        return False, str(e)


def _nifi_stop() -> Tuple[bool, str]:
    nifi_sh = _nifi_home() / "bin" / "nifi.sh"
    if not nifi_sh.exists():
        return False, f"No encuentro nifi.sh en: {nifi_sh}"
    try:
        r = _safe_run([str(nifi_sh), "stop"], timeout_s=120)
        time.sleep(1)
        chk = _nifi_is_running()
        return bool(r["rc"] == 0) and (not chk["running"]), (r["stdout"] + "\n" + r["stderr"]).strip()
    except Exception as e:
        return False, str(e)


def _nifi_status() -> Dict[str, Any]:
    nifi_sh = _nifi_home() / "bin" / "nifi.sh"
    r = _safe_run([str(nifi_sh), "status"], timeout_s=60)
    chk = _nifi_is_running()
    return {"running": chk["running"], "ports": chk["ports"], "rc": r["rc"], "output": (r["stdout"] + r["stderr"]).strip()}


def _nifi_required_processors_fase1() -> List[Dict[str, str]]:
    """
    Lista conceptual de procesadores típicos para cumplir la Fase I con NiFi:
    (OpenWeather/ElectricityMaps) -> Transformación -> PutKafka -> (opcional) PutHDFS.
    """
    return [
        {
            "procesador": "GenerateFlowFile (Trigger)",
            "objetivo": "Disparar la ejecución periódica (p. ej. cada 15 min).",
        },
        {
            "procesador": "InvokeHTTP (OpenWeather/ElectricityMaps)",
            "objetivo": "Consumir APIs externas y obtener JSON con telemetría (clima) y/o carbono/mix.",
        },
        {
            "procesador": "EvaluateJsonPath (opcional)",
            "objetivo": "Extraer campos concretos del JSON para usarlos como atributos o para construir el payload.",
        },
        {
            "procesador": "JoltTransformJSON",
            "objetivo": "Transformar el JSON a la estructura esperada por `energy_raw` y `weather_raw`.",
        },
        {
            "procesador": "PublishKafka (por topic)",
            "objetivo": "Publicar a Kafka (2 instancias): `weather_raw` y `energy_raw`.",
        },
        {
            "procesador": "PutHDFS (opcional)",
            "objetivo": "Guardar raw en HDFS para auditoría/back-up (si quieres cumplir 100% el PDF).",
        },
        {
            "procesador": "Controller Service Kafka (necesario por PublishKafka)",
            "objetivo": "Configurar conexión a Kafka (bootstrap servers) que usa `PublishKafka`.",
        },
    ]


def _nifi_rest() -> str:
    return "https://localhost:8443/nifi-api"


def _nifi_auth_headers() -> Dict[str, str]:
    """Headers con Bearer token si hay credenciales NiFi."""
    user = os.environ.get("NIFI_USER")
    passwd = os.environ.get("NIFI_PASS")
    if not user or not passwd:
        return {}
    try:
        r = requests.post(
            f"{_nifi_rest()}/access/token",
            data={"username": user, "password": passwd},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            verify=False,
            timeout=10,
        )
        if r.status_code in (200, 201) and r.text:
            return {"Authorization": f"Bearer {r.text.strip()}"}
    except Exception:
        pass
    return {}


def _nifi_get_root_group_id() -> Optional[str]:
    try:
        r = requests.get(
            f"{_nifi_rest()}/flow/process-groups/root",
            headers=_nifi_auth_headers(),
            verify=False,
            timeout=10,
        )
        if r.status_code != 200:
            return None
        return r.json()["processGroupFlow"]["id"]
    except Exception:
        return None


def _nifi_list_processors(pg_id: str) -> Dict[str, str]:
    """
    Devuelve mapa name -> id.
    """
    try:
        r = requests.get(
            f"{_nifi_rest()}/process-groups/{pg_id}/processors",
            headers=_nifi_auth_headers(),
            verify=False,
            timeout=15,
        )
        if r.status_code != 200:
            return {}
        procs = r.json().get("processors", [])
        out = {}
        for p in procs:
            name = (p.get("component") or {}).get("name") or p.get("name") or ""
            pid = p.get("id") or ""
            if name and pid:
                out[name] = pid
        return out
    except Exception:
        return {}


def _nifi_create_processor(
    pg_id: str,
    proc_type: str,
    name: str,
    x: float,
    y: float,
    props: Optional[Dict[str, Any]] = None,
) -> Tuple[bool, str]:
    body: Dict[str, Any] = {
        "revision": {"clientId": f"cursor-{int(time.time()*1000)}", "version": 0},
        "component": {
            "type": proc_type,
            "name": name,
            "position": {"x": float(x), "y": float(y)},
        },
    }
    if props:
        body["component"]["config"] = {"properties": props}

    try:
        r = requests.post(
            f"{_nifi_rest()}/process-groups/{pg_id}/processors",
            headers={**_nifi_auth_headers(), "Content-Type": "application/json"},
            json=body,
            verify=False,
            timeout=30,
        )
        if r.status_code in (200, 201):
            return True, f"{name} creado."
        return False, f"No se pudo crear {name}. HTTP {r.status_code}: {r.text[:2000]}"
    except Exception as e:
        return False, f"Error creando {name}: {e}"


def _nifi_crear_procesadores_fase1_demo() -> Dict[str, Any]:
    """
    Crea en NiFi los procesadores mínimos para la Fase I (demo), sin asegurar
    que queden RUNNING ni que estén conectados.
    """
    pg_id = _nifi_get_root_group_id()
    if not pg_id:
        return {"ok": False, "error": "No pude obtener el root process group id en NiFi."}

    existing = _nifi_list_processors(pg_id)

    # Class names confirmados en esta NiFi.
    to_create = [
        (
            "org.apache.nifi.processors.standard.GenerateFlowFile",
            "NiFi_F1_GenerateTrigger",
            150.0,
            150.0,
            {"generate-ff-custom-text": "Fase1 Trigger (NiFi demo)"},
        ),
        (
            "org.apache.nifi.processors.standard.InvokeHTTP",
            "NiFi_F1_InvokeHTTP_OpenWeather",
            450.0,
            150.0,
            {
                "HTTP Method": "GET",
                # Placeholder: si tienes API_WEATHER_KEY lo puedes inyectar via EL.
                "HTTP URL": "https://api.openweathermap.org/data/2.5/weather?q=Madrid&appid=${API_WEATHER_KEY}",
            },
        ),
        (
            "org.apache.nifi.processors.jolt.JoltTransformJSON",
            "NiFi_F1_JoltTransformJSON_ToSchema",
            750.0,
            150.0,
            {
                "Jolt Transform": "jolt-transform-sort",
                # Con jolt-transform-sort, Jolt Specification no es obligatorio.
            },
        ),
        (
            "org.apache.nifi.kafka.processors.PublishKafka",
            "NiFi_F1_PublishKafka_weather_raw",
            1050.0,
            150.0,
            {"Topic Name": "weather_raw"},
        ),
        (
            "org.apache.nifi.kafka.processors.PublishKafka",
            "NiFi_F1_PublishKafka_energy_raw",
            1050.0,
            350.0,
            {"Topic Name": "energy_raw"},
        ),
        (
            "org.apache.nifi.processors.standard.ExecuteStreamCommand",
            "NiFi_F1_ExecuteProducer",
            150.0,
            450.0,
            {
                "Command": sys.executable or "python3",
                "Command Arguments": "producer.py",
                "Working Directory": str(BASE),
            },
        ),
        (
            "org.apache.nifi.processors.standard.GetFile",
            "NiFi_F1_GetFile_GPS",
            150.0,
            550.0,
            {
                "Input Directory": NIFI_GPS_LOGS_DIR,
                "Keep Source File": "true",
            },
        ),
        (
            "org.apache.nifi.kafka.processors.PublishKafka",
            "NiFi_F1_PublishKafka_gps_raw",
            450.0,
            550.0,
            {"Topic Name": TOPIC_GPS_RAW},
        ),
    ]

    created: List[Dict[str, str]] = []
    skipped: List[str] = []

    for proc_type, name, x, y, props in to_create:
        if name in existing:
            skipped.append(name)
            continue
        ok, msg = _nifi_create_processor(pg_id, proc_type, name, x, y, props=props)
        if ok:
            created.append({"name": name, "type": proc_type})
        else:
            return {"ok": False, "error": msg, "created": created, "skipped": skipped}

    return {"ok": True, "created": created, "skipped": skipped, "rootGroupId": pg_id}


def _nifi_conectar_y_configurar_fase1() -> Dict[str, Any]:
    """
    Conecta los procesadores NiFi Fase I demo: crea Kafka3ConnectionService si falta,
    configura PublishKafka con el controller service, crea conexiones y habilita el CS.
    """
    pg_id = _nifi_get_root_group_id()
    if not pg_id:
        return {"ok": False, "error": "No pude obtener el root process group id en NiFi."}

    existing = _nifi_list_processors(pg_id)
    required_names = [
        "NiFi_F1_GenerateTrigger",
        "NiFi_F1_InvokeHTTP_OpenWeather",
        "NiFi_F1_JoltTransformJSON_ToSchema",
        "NiFi_F1_PublishKafka_weather_raw",
        "NiFi_F1_PublishKafka_energy_raw",
    ]
    optional_names = [
        "NiFi_F1_ExecuteProducer",
        "NiFi_F1_GetFile_GPS",
        "NiFi_F1_PublishKafka_gps_raw",
    ]
    missing = [n for n in required_names if n not in existing]
    if missing:
        return {"ok": False, "error": f"Procesadores faltantes. Crea primero con 'Crear procesadores NiFi (Fase I demo)': {missing}"}

    base_url = _nifi_rest()
    client_id = f"cursor-connect-{int(time.time()*1000)}"

    # 1) Crear Kafka3ConnectionService si no existe
    try:
        r = requests.get(
            f"{base_url}/flow/process-groups/{pg_id}/controller-services",
            headers=_nifi_auth_headers(),
            verify=False,
            timeout=15,
        )
        cs_list = (r.json() if r.status_code == 200 else {}).get("controllerServices", [])
        kafka_cs = None
        for cs in cs_list:
            comp = cs.get("component") or {}
            if comp.get("type") == "org.apache.nifi.kafka.service.Kafka3ConnectionService":
                kafka_cs = cs.get("id") or comp.get("id")
                break

        if not kafka_cs:
            create_cs = {
                "revision": {"clientId": client_id, "version": 0},
                "disconnectedNodeAcknowledged": False,
                "component": {
                    "name": "KafkaConnService_demo",
                    "type": "org.apache.nifi.kafka.service.Kafka3ConnectionService",
                    "bundle": {"group": "org.apache.nifi", "artifact": "nifi-kafka-3-service-nar", "version": "2.6.0"},
                    "state": "DISABLED",
                    "properties": {"bootstrap.servers": KAFKA_BOOTSTRAP},
                },
            }
            r2 = requests.post(
                f"{base_url}/process-groups/{pg_id}/controller-services",
                json=create_cs,
                headers={**_nifi_auth_headers(), "Content-Type": "application/json"},
                verify=False,
                timeout=30,
            )
            if r2.status_code not in (200, 201):
                return {"ok": False, "error": f"No se pudo crear Kafka Connection Service: {r2.status_code} {r2.text[:500]}"}
            kafka_cs = r2.json().get("id")
            # Corregir bootstrap.servers si NiFi guardó bootstrapServers por error
            cur = r2.json()
            props = (cur.get("component") or {}).get("properties") or {}
            if not props.get("bootstrap.servers"):
                upd = {
                    "revision": cur["revision"],
                    "component": {"id": kafka_cs, "properties": {"bootstrap.servers": KAFKA_BOOTSTRAP}},
                }
                requests.put(f"{base_url}/controller-services/{kafka_cs}", json=upd, headers=_nifi_auth_headers(), verify=False, timeout=15)
    except Exception as e:
        return {"ok": False, "error": f"Error creando/configurando Kafka CS: {e}"}

    # Esperar a que el CS esté válido; habilitarlo
    for _ in range(15):
        try:
            r = requests.get(f"{base_url}/controller-services/{kafka_cs}", headers=_nifi_auth_headers(), verify=False, timeout=10)
            if r.status_code != 200:
                time.sleep(2)
                continue
            j = r.json()
            rev = j["revision"]["version"]
            state = (j.get("component") or {}).get("state") or "DISABLED"
            if state == "DISABLED":
                req = requests.put(
                    f"{base_url}/controller-services/{kafka_cs}",
                    json={"revision": {"clientId": client_id, "version": rev}, "component": {"id": kafka_cs, "state": "ENABLED"}},
                    headers=_nifi_auth_headers(),
                    verify=False,
                    timeout=15,
                )
                if req.status_code != 200:
                    time.sleep(2)
                    continue
            elif state in ("ENABLING", "ENABLED"):
                break
            time.sleep(2)
        except Exception:
            time.sleep(2)
    time.sleep(3)

    # 2) Actualizar config de procesadores (Kafka CS, autoTerminate, schedulingPeriod)
    pid_map = {n: existing[n] for n in required_names + optional_names if n in existing}
    updates = {
        pid_map["NiFi_F1_GenerateTrigger"]: {"schedulingPeriod": "10 sec", "auto": [], "props": {}},
        pid_map["NiFi_F1_InvokeHTTP_OpenWeather"]: {"schedulingPeriod": "10 sec", "auto": ["Failure", "No Retry", "Response", "Retry"], "props": {}},
        pid_map["NiFi_F1_JoltTransformJSON_ToSchema"]: {"schedulingPeriod": "10 sec", "auto": ["failure"], "props": {}},
        pid_map["NiFi_F1_PublishKafka_weather_raw"]: {"schedulingPeriod": "10 sec", "auto": ["success", "failure"], "props": {"Kafka Connection Service": kafka_cs}},
        pid_map["NiFi_F1_PublishKafka_energy_raw"]: {"schedulingPeriod": "10 sec", "auto": ["success", "failure"], "props": {"Kafka Connection Service": kafka_cs}},
    }
    for k in ["NiFi_F1_ExecuteProducer", "NiFi_F1_GetFile_GPS", "NiFi_F1_PublishKafka_gps_raw"]:
        if k in pid_map:
            if k == "NiFi_F1_ExecuteProducer":
                updates[pid_map[k]] = {"schedulingPeriod": "15 min", "auto": ["output-stream", "stderr"], "props": {}}
            elif k == "NiFi_F1_GetFile_GPS":
                updates[pid_map[k]] = {"schedulingPeriod": "1 min", "auto": ["success", "failure"], "props": {}}
            else:
                updates[pid_map[k]] = {"schedulingPeriod": "10 sec", "auto": ["success", "failure"], "props": {"Kafka Connection Service": kafka_cs}}
    for pid, upd in updates.items():
        try:
            cur = requests.get(f"{base_url}/processors/{pid}", headers=_nifi_auth_headers(), verify=False, timeout=15).json()
            # Verificar si está RUNNING y detenerlo antes de modificar config
            was_running = (cur.get("component") or {}).get("state") == "RUNNING"
            if was_running:
                # Detener el procesador
                stop_req = requests.put(
                    f"{base_url}/processors/{pid}/run-status",
                    json={"state": "STOPPED", "revision": cur["revision"]},
                    headers=_nifi_auth_headers(),
                    verify=False,
                    timeout=15,
                )
                if stop_req.status_code != 200:
                    return {"ok": False, "error": f"No se pudo detener {pid} para actualizar: {stop_req.text[:300]}"}
                # Refrescar revision tras detener
                time.sleep(0.5)
                cur = requests.get(f"{base_url}/processors/{pid}", headers=_nifi_auth_headers(), verify=False, timeout=15).json()
            
            cfg = copy.deepcopy(cur["component"]["config"])
            cfg["schedulingPeriod"] = upd["schedulingPeriod"]
            cfg["autoTerminatedRelationships"] = upd["auto"]
            props = copy.deepcopy(cfg.get("properties") or {})
            props.update(upd["props"])
            cfg["properties"] = props
            req = requests.put(
                f"{base_url}/processors/{pid}",
                json={"revision": cur["revision"], "component": {"id": pid, "config": cfg}},
                headers=_nifi_auth_headers(),
                verify=False,
                timeout=15,
            )
            if req.status_code >= 400:
                return {"ok": False, "error": f"Error actualizando {pid}: {req.text[:300]}"}
            
            # Si estaba RUNNING, volver a arrancarlo
            if was_running:
                time.sleep(0.5)
                updated = requests.get(f"{base_url}/processors/{pid}", headers=_nifi_auth_headers(), verify=False, timeout=15).json()
                start_req = requests.put(
                    f"{base_url}/processors/{pid}/run-status",
                    json={"state": "RUNNING", "revision": updated["revision"]},
                    headers=_nifi_auth_headers(),
                    verify=False,
                    timeout=15,
                )
                if start_req.status_code != 200:
                    # No fallar si no se puede arrancar, solo avisar
                    pass
        except Exception as e:
            return {"ok": False, "error": f"Error actualizando procesador {pid}: {e}"}

    # 3) Crear conexiones si no existen
    try:
        r = requests.get(f"{base_url}/process-groups/{pg_id}/connections", headers=_nifi_auth_headers(), verify=False, timeout=15)
        conns = (r.json() if r.status_code == 200 else {}).get("connections", [])
        conn_count = len(conns)
    except Exception:
        conn_count = 0

    links = [
        (pid_map["NiFi_F1_GenerateTrigger"], "success", pid_map["NiFi_F1_InvokeHTTP_OpenWeather"]),
        (pid_map["NiFi_F1_InvokeHTTP_OpenWeather"], "Original", pid_map["NiFi_F1_JoltTransformJSON_ToSchema"]),
        (pid_map["NiFi_F1_JoltTransformJSON_ToSchema"], "success", pid_map["NiFi_F1_PublishKafka_weather_raw"]),
        (pid_map["NiFi_F1_JoltTransformJSON_ToSchema"], "success", pid_map["NiFi_F1_PublishKafka_energy_raw"]),
    ]
    if "NiFi_F1_ExecuteProducer" in pid_map:
        links.append((pid_map["NiFi_F1_GenerateTrigger"], "success", pid_map["NiFi_F1_ExecuteProducer"]))
    if "NiFi_F1_GetFile_GPS" in pid_map and "NiFi_F1_PublishKafka_gps_raw" in pid_map:
        links.append((pid_map["NiFi_F1_GetFile_GPS"], "success", pid_map["NiFi_F1_PublishKafka_gps_raw"]))
    if conn_count < len(links):
        for src_id, rel, dst_id in links:
            try:
                body = {
                    "revision": {"clientId": client_id, "version": 0},
                    "component": {
                        "source": {"id": src_id, "groupId": pg_id, "type": "PROCESSOR"},
                        "destination": {"id": dst_id, "groupId": pg_id, "type": "PROCESSOR"},
                        "selectedRelationships": [rel],
                    },
                }
                req = requests.post(
                    f"{base_url}/process-groups/{pg_id}/connections",
                    json=body,
                    headers={**_nifi_auth_headers(), "Content-Type": "application/json"},
                    verify=False,
                    timeout=20,
                )
                if req.status_code not in (200, 201):
                    return {"ok": False, "error": f"No se pudo crear conexión {rel}: {req.text[:300]}"}
            except Exception as e:
                return {"ok": False, "error": f"Error creando conexión: {e}"}

    return {"ok": True, "message": "Procesadores conectados y configurados. Kafka CS habilitado."}


def _nifi_arrancar_procesadores_fase1() -> Dict[str, Any]:
    """Pone en RUNNING los procesadores NiFi Fase I demo."""
    pg_id = _nifi_get_root_group_id()
    if not pg_id:
        return {"ok": False, "error": "No pude obtener el root process group id en NiFi."}
    existing = _nifi_list_processors(pg_id)
    names = ["NiFi_F1_GenerateTrigger", "NiFi_F1_InvokeHTTP_OpenWeather", "NiFi_F1_JoltTransformJSON_ToSchema",
             "NiFi_F1_PublishKafka_weather_raw", "NiFi_F1_PublishKafka_energy_raw",
             "NiFi_F1_ExecuteProducer", "NiFi_F1_GetFile_GPS", "NiFi_F1_PublishKafka_gps_raw"]
    pids = [existing[n] for n in names if n in existing]
    if len(pids) < 5:
        return {"ok": False, "error": f"Faltan procesadores base. Necesitas crear/conectar primero: {[n for n in names[:5] if n not in existing]}"}

    base_url = _nifi_rest()
    client_id = f"cursor-start-{int(time.time()*1000)}"
    started = []
    for pid in pids:
        try:
            cur = requests.get(f"{base_url}/processors/{pid}", headers=_nifi_auth_headers(), verify=False, timeout=10).json()
            if (cur.get("component") or {}).get("state") == "RUNNING":
                started.append(pid)
                continue
            r = requests.put(
                f"{base_url}/processors/{pid}/run-status",
                json={"state": "RUNNING", "revision": cur["revision"]},
                headers=_nifi_auth_headers(),
                verify=False,
                timeout=15,
            )
            if r.status_code == 200:
                started.append(pid)
            else:
                return {"ok": False, "error": f"Error arrancando {pid}: {r.text[:200]}", "started": started}
        except Exception as e:
            return {"ok": False, "error": str(e), "started": started}
    return {"ok": True, "message": f"Procesadores en RUNNING ({len(started)}).", "started": started}


def _nifi_parar_procesadores_fase1() -> Dict[str, Any]:
    """Pone en STOPPED los procesadores NiFi Fase I demo."""
    pg_id = _nifi_get_root_group_id()
    if not pg_id:
        return {"ok": False, "error": "No pude obtener el root process group id en NiFi."}
    existing = _nifi_list_processors(pg_id)
    names = ["NiFi_F1_GenerateTrigger", "NiFi_F1_InvokeHTTP_OpenWeather", "NiFi_F1_JoltTransformJSON_ToSchema",
             "NiFi_F1_PublishKafka_weather_raw", "NiFi_F1_PublishKafka_energy_raw",
             "NiFi_F1_ExecuteProducer", "NiFi_F1_GetFile_GPS", "NiFi_F1_PublishKafka_gps_raw"]
    pids = [existing[n] for n in names if n in existing]
    if not pids:
        return {"ok": True, "message": "No hay procesadores Fase I para parar."}

    base_url = _nifi_rest()
    stopped = []
    for pid in pids:
        try:
            cur = requests.get(f"{base_url}/processors/{pid}", headers=_nifi_auth_headers(), verify=False, timeout=10).json()
            if (cur.get("component") or {}).get("state") == "STOPPED":
                stopped.append(pid)
                continue
            r = requests.put(
                f"{base_url}/processors/{pid}/run-status",
                json={"state": "STOPPED", "revision": cur["revision"]},
                headers=_nifi_auth_headers(),
                verify=False,
                timeout=15,
            )
            if r.status_code == 200:
                stopped.append(pid)
            else:
                return {"ok": False, "error": f"Error parando {pid}: {r.text[:200]}", "stopped": stopped}
        except Exception as e:
            return {"ok": False, "error": str(e), "stopped": stopped}
    return {"ok": True, "message": f"Procesadores parados ({len(stopped)}/{len(pids)}).", "stopped": stopped}


def _normalizar_estado(estado: Optional[str]) -> str:
    if not estado:
        return "ok"
    e = str(estado).strip().lower()
    if e in ("ok", "fluido", "normal"):
        return "ok"
    if e in ("alerta", "congestionado", "congestionada"):
        return "alerta"
    if e in ("sobrecarga", "bloqueado", "bloqueada"):
        return "sobrecarga"
    return "ok"


def _color_por_estado(estado_norm: str) -> str:
    return COLORES_ESTADO.get(estado_norm, COLOR_DEFAULT)


@st.cache_resource
def _cluster_cassandra(contact_points: Tuple[str, ...], user: str, password: str):
    if Cluster is None:
        return None
    auth = None
    if user and password:
        auth = PlainTextAuthProvider(user, password)
    return Cluster(list(contact_points), auth_provider=auth)


def obtener_session_cassandra(host: str, user: str = "", password: str = ""):
    """Sesión al keyspace smart_grid o None si falla."""
    if Cluster is None:
        return None
    try:
        c = _cluster_cassandra((host.strip() or CASSANDRA_HOST,), user, password)
        if c is None:
            return None
        return c.connect(KEYSPACE)
    except Exception:
        return None


def cargar_subestaciones(session) -> Dict[str, Dict[str, Any]]:
    if not session:
        return {}
    try:
        rows = session.execute(
            """
            SELECT id_subestacion, lat, lon, voltaje_kv, potencia_mw, capacidad_mw,
                   uso_pct, estado, motivo, clima_actual, temperatura, humedad,
                   ultima_actualizacion
            FROM subestaciones_estado
            """
        )
        out = {}
        for r in rows:
            out[r.id_subestacion] = {
                "lat": r.lat,
                "lon": r.lon,
                "voltaje_kv": r.voltaje_kv,
                "potencia_mw": r.potencia_mw,
                "capacidad_mw": r.capacidad_mw,
                "uso_pct": r.uso_pct,
                "estado": _normalizar_estado(r.estado),
                "motivo": r.motivo or "",
                "clima": r.clima_actual or "",
                "temperatura": r.temperatura,
                "humedad": r.humedad,
                "ultima_actualizacion": r.ultima_actualizacion,
            }
        return out
    except Exception:
        return {}


def cargar_lineas(session) -> Dict[str, Dict[str, Any]]:
    if not session:
        return {}
    try:
        rows = session.execute(
            "SELECT src, dst, flujo_mw, capacidad_mw, estado FROM lineas_estado"
        )
        return {
            f"{r.src}|{r.dst}": {
                "estado": _normalizar_estado(r.estado),
                "flujo_mw": r.flujo_mw,
                "capacidad_mw": r.capacidad_mw,
            }
            for r in rows
        }
    except Exception:
        return {}


def cargar_pagerank(session) -> Dict[str, float]:
    if not session:
        return {}
    try:
        rows = session.execute(
            "SELECT id_subestacion, pagerank FROM pagerank_subestaciones"
        )
        return {r.id_subestacion: float(r.pagerank or 0) for r in rows}
    except Exception:
        return {}


def cargar_puntos_fallo(session) -> List[Dict[str, Any]]:
    if not session:
        return []
    try:
        rows = session.execute(
            """
            SELECT id_subestacion, es_articulacion, fragmentos_al_fallar, nodos_afectados_json
            FROM puntos_fallo_unicos
            WHERE es_articulacion = true ALLOW FILTERING
            """
        )
        return [
            {
                "id": r.id_subestacion,
                "fragmentos": r.fragmentos_al_fallar,
                "detalle": (r.nodos_afectados_json or "")[:500],
            }
            for r in rows
        ]
    except Exception:
        return []


def datos_demo() -> Dict[str, Dict[str, Any]]:
    nodos = get_nodos()
    return {
        n: {
            "lat": d["lat"],
            "lon": d["lon"],
            "voltaje_kv": 220.0,
            "potencia_mw": 0.0,
            "capacidad_mw": float(d.get("capacidad_mw", 200)),
            "uso_pct": 0.0,
            "estado": "ok",
            "motivo": "",
            "clima": "Sin datos (ejecuta producer + Spark o conecta Cassandra)",
            "temperatura": None,
            "humedad": None,
            "ultima_actualizacion": None,
        }
        for n, d in nodos.items()
    }


def construir_mapa(
    subestaciones: Dict[str, Dict[str, Any]],
    lineas: Dict[str, Dict[str, Any]],
    pagerank: Dict[str, float],
) -> folium.Map:
    m = folium.Map(location=[40.4, -3.7], zoom_start=6, tiles="OpenStreetMap")
    nodos_topo = get_nodos()
    aristas = get_aristas()

    for t in aristas:
        src, dst = t[0], t[1]
        if src not in nodos_topo or dst not in nodos_topo:
            continue
        key, key_inv = f"{src}|{dst}", f"{dst}|{src}"
        est = lineas.get(key) or lineas.get(key_inv) or {}
        estado_lin = est.get("estado", "ok")
        flujo = est.get("flujo_mw")
        cap = est.get("capacidad_mw")
        tip = f"{src} ↔ {dst} | {estado_lin.upper()}"
        if flujo is not None and cap:
            tip += f" | {flujo:.0f}/{cap:.0f} MW"
        folium.PolyLine(
            [
                [nodos_topo[src]["lat"], nodos_topo[src]["lon"]],
                [nodos_topo[dst]["lat"], nodos_topo[dst]["lon"]],
            ],
            color=_color_por_estado(estado_lin),
            weight=3,
            opacity=0.75,
            tooltip=tip,
        ).add_to(m)

    for nid, datos_topo in nodos_topo.items():
        est = subestaciones.get(nid) or {}
        estado = est.get("estado", "ok")
        pr = float(pagerank.get(nid, 0) or 0)
        volt = est.get("voltaje_kv", 220)
        pot = est.get("potencia_mw", 0)
        uso = est.get("uso_pct", 0)
        cap = est.get("capacidad_mw") or datos_topo.get("capacidad_mw", 200)
        popup_html = (
            f"<b>{nid}</b> ({datos_topo.get('tipo', '')})<br>"
            f"<b>Estado:</b> {estado.upper()}<br>"
            f"<b>Voltaje:</b> {volt} kV<br>"
            f"<b>Potencia:</b> {pot} MW / {cap} MW cap.<br>"
            f"<b>Uso:</b> {uso}%<br>"
            f"<b>PageRank:</b> {pr:.4f}<br>"
            f"<small>{est.get('motivo') or '—'}</small>"
        )
        folium.CircleMarker(
            location=[datos_topo["lat"], datos_topo["lon"]],
            radius=14 if datos_topo.get("tipo") == "principal" else 8,
            color=_color_por_estado(estado),
            fill=True,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=280),
        ).add_to(m)

    folium.LayerControl().add_to(m)
    return m


def _generar_informe_cambios_ciclo(
    prev: Dict[str, Any],
    curr: Dict[str, Any],
) -> Dict[str, Any]:
    """Compara snapshot previo con actual y devuelve informe de cambios."""
    out: Dict[str, Any] = {"hay_cambios": False, "texto": "", "cambios_estado": []}
    prev_sub = prev.get("subestaciones") or {}
    curr_sub = curr.get("subestaciones") or {}
    prev_pr = prev.get("pagerank") or {}
    curr_pr = curr.get("pagerank") or {}
    prev_art = prev.get("articulacion_ids") or set()
    curr_art = curr.get("articulacion_ids") or set()
    if isinstance(prev_art, list):
        prev_art = set(prev_art)
    if isinstance(curr_art, list):
        curr_art = set(curr_art)

    lineas: List[str] = []
    cambios_estado: List[Dict[str, Any]] = []

    # Cambios de estado en subestaciones
    for sid, s in curr_sub.items():
        p = prev_sub.get(sid, {})
        est_prev = p.get("estado") or "—"
        est_curr = s.get("estado") or "—"
        if est_prev != est_curr:
            cambios_estado.append({
                "Subestación": sid,
                "Antes": est_prev,
                "Después": est_curr,
                "Potencia (MW)": s.get("potencia_mw"),
            })
    if cambios_estado:
        lineas.append(f"**{len(cambios_estado)} subestación(es) cambiaron de estado.**")
        out["cambios_estado"] = cambios_estado

    # KPIs
    d_alert = curr.get("n_alert", 0) - prev.get("n_alert", 0)
    d_sobre = curr.get("n_sobre", 0) - prev.get("n_sobre", 0)
    d_pot = (curr.get("pot_total") or 0) - (prev.get("pot_total") or 0)
    if d_alert != 0 or d_sobre != 0 or abs(d_pot) > 0.01:
        lineas.append(f"**KPIs:** Alertas {prev.get('n_alert', 0)} → {curr.get('n_alert', 0)} ({d_alert:+d}), "
                     f"Sobrecargas {prev.get('n_sobre', 0)} → {curr.get('n_sobre', 0)} ({d_sobre:+d}), "
                     f"Potencia total {prev.get('pot_total', 0):,.0f} → {curr.get('pot_total', 0):,.0f} MW ({d_pot:+,.0f}).")

    # PageRank: top 3 que más subieron/bajaron
    diff_pr: List[Tuple[str, float]] = []
    all_ids = set(prev_pr.keys()) | set(curr_pr.keys())
    for sid in all_ids:
        pv = prev_pr.get(sid, 0.0)
        cv = curr_pr.get(sid, 0.0)
        d = cv - pv
        if abs(d) > 1e-6:
            diff_pr.append((sid, d))
    if diff_pr:
        diff_pr.sort(key=lambda x: -x[1])
        suben = [x for x in diff_pr if x[1] > 0][:3]
        bajan = [x for x in diff_pr if x[1] < 0][:3]
        if suben or bajan:
            parts = []
            if suben:
                parts.append("PageRank ↑: " + ", ".join(f"{n}({d:.4f})" for n, d in suben))
            if bajan:
                parts.append("PageRank ↓: " + ", ".join(f"{n}({d:.4f})" for n, d in bajan))
            lineas.append("**" + " · ".join(parts) + "**")

    # Articulaciones
    nuevas = curr_art - prev_art
    perdidas = prev_art - curr_art
    if nuevas or perdidas:
        lineas.append(f"**Puntos de fallo:** {len(nuevas)} nuevos, {len(perdidas)} ya no críticos.")

    out["hay_cambios"] = bool(lineas)
    out["texto"] = "\n\n".join(lineas) if lineas else "Sin cambios detectados."
    return out


def ejecutar_ciclo_pipeline() -> None:
    paso = int(st.session_state.get("paso_15min", 0))
    env = {**os.environ, "PASO_15MIN": str(paso)}

    with st.spinner("Producer (ingesta)…"):
        r1 = subprocess.run(
            [sys.executable, str(BASE / "producer.py")],
            env=env,
            capture_output=True,
            text=True,
            cwd=str(BASE),
            timeout=120,
        )
    if r1.returncode != 0:
        st.error("Fallo en **producer.py**")
        with st.expander("Salida del producer"):
            st.code((r1.stderr or r1.stdout or "")[:8000])
        return

    with st.spinner("Spark (procesamiento_grafos)…"):
        r2 = subprocess.run(
            [
                sys.executable,
                str(BASE / "procesamiento" / "procesamiento_grafos.py"),
            ],
            env=env,
            capture_output=True,
            text=True,
            cwd=str(BASE),
            timeout=300,
        )
    if r2.returncode != 0:
        st.error("Fallo en **procesamiento_grafos.py** (¿Cassandra en 9042?)")
        with st.expander("Salida Spark"):
            st.code((r2.stderr or r2.stdout or "")[:8000])
        return

    st.session_state["paso_15min"] = paso + 1
    _cluster_cassandra.clear()
    st.success("Ciclo completado.")
    st.rerun()


# Consultas Hive para cuadro de mando directivo (histórico)
_CUADRO_MANDO_QUERIES: List[Tuple[str, str, str]] = [
    ("Consumo energético total (MWh)", "consumo", f"""
        SELECT id_subestacion, SUM(energia_mwh) AS total_mwh, AVG(potencia_max_mw) AS potencia_media_mw
        FROM {HIVE_DB}.consumo_energetico_diario GROUP BY id_subestacion ORDER BY total_mwh DESC LIMIT 15;
    """),
    ("Sostenibilidad — carbono y renovables", "sostenibilidad", f"""
        SELECT fecha, carbon_intensity_g_co2_kwh, renewable_pct, carga_media_subestaciones_mw
        FROM {HIVE_DB}.sostenibilidad_carbono_hist ORDER BY fecha DESC LIMIT 20;
    """),
    ("Eventos de red (alertas y sobrecargas)", "red", f"""
        SELECT origen, destino, estado, motivo_fallo, flujo_mw, timestamp_evento
        FROM {HIVE_DB}.red_electrica_hist WHERE estado != 'OK' ORDER BY timestamp_evento DESC LIMIT 15;
    """),
    ("Nodos críticos (PageRank histórico)", "pagerank", f"""
        SELECT id_subestacion, pagerank_score, voltaje_kv, potencia_mw, fecha_proceso
        FROM {HIVE_DB}.metricas_subestaciones_hist ORDER BY pagerank_score DESC LIMIT 15;
    """),
    ("Consumo por fecha (últimos registros)", "consumo_fecha", f"""
        SELECT id_subestacion, fecha, energia_mwh, num_eventos_sobrecarga, num_eventos_alerta
        FROM {HIVE_DB}.consumo_energetico_diario ORDER BY fecha DESC LIMIT 20;
    """),
    ("Clima en subestaciones", "clima", f"""
        SELECT subestacion_nombre, temperatura, humedad, descripcion, fecha_captura
        FROM {HIVE_DB}.clima_hist ORDER BY fecha_captura DESC LIMIT 15;
    """),
    ("Subestaciones histórico (persistencia)", "subest_hist", f"""
        SELECT id_subestacion, voltaje_kv, potencia_mw, estado, timestamp FROM {HIVE_DB}.subestaciones_historico LIMIT 20;
    """),
    ("Eventos de red histórico", "eventos_hist", f"""
        SELECT tipo_entidad, id_entidad, estado, motivo, timestamp FROM {HIVE_DB}.eventos_red_historico ORDER BY timestamp DESC LIMIT 15;
    """),
    ("Tablas disponibles", "tablas", f"USE {HIVE_DB}; SHOW TABLES;"),
]


def _render_cuadro_mando_directivo() -> None:
    """Cuadro de mando para directivos: KPIs y reportes desde Hive (histórico)."""
    st.markdown("### 📊 Cuadro de mando directivo")
    st.caption(
        "Reportes históricos desde Hive. Los datos provienen del pipeline (producer → Spark → persistencia_hive). "
        "Pulsa un botón para cargar el informe."
    )
    if "cuadro_mando_result" not in st.session_state:
        st.session_state["cuadro_mando_result"] = None
        st.session_state["cuadro_mando_label"] = ""
    st.markdown("##### Informes desde el histórico (Hive)")
    cols = st.columns(3)
    for i, (label, key, sql) in enumerate(_CUADRO_MANDO_QUERIES):
        with cols[i % 3]:
            if st.button(label, key=f"cuadro_{key}", use_container_width=True):
                with st.spinner(f"Consultando Hive: {label}..."):
                    rc, out = _ejecutar_consulta_hive(sql.strip())
                parsed = _parse_hive_spark_sql_cli_output(out) if rc == 0 else None
                st.session_state["cuadro_mando_result"] = (rc, out, parsed)
                st.session_state["cuadro_mando_label"] = label
                st.rerun()
    res = st.session_state.get("cuadro_mando_result")
    if res:
        rc, out, parsed = res
        label = st.session_state.get("cuadro_mando_label", "")
        st.markdown(f"##### 📋 {label}")
        if rc == 0:
            if parsed:
                st.dataframe(parsed, use_container_width=True, hide_index=True)
            else:
                st.info("Sin filas o formato no reconocido.")
            with st.expander("Salida raw"):
                st.code(out[:6000] if out else "(vacío)")
        else:
            st.warning(f"rc={rc}. Ver salida.")
            with st.expander("Salida"):
                st.code((out or "")[:4000])
        if st.button("Limpiar resultado", key="cuadro_clear"):
            st.session_state["cuadro_mando_result"] = None
            st.session_state["cuadro_mando_label"] = ""
            st.rerun()
    st.divider()
    st.markdown("##### Resumen de tablas del histórico")
    st.caption(
        "Tablas en Hive: consumo_energetico_diario, sostenibilidad_carbono_hist, red_electrica_hist, "
        "metricas_subestaciones_hist, clima_hist, clima_renovables_hist; "
        "persistencia_hive añade: subestaciones_historico, lineas_historico, eventos_red_historico."
    )


def main():
    st.set_page_config(
        page_title="Smart Grid España",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("Smart Grid — Monitor de red eléctrica")

    with st.sidebar:
        st.header("Conexión")
        host = st.text_input("Cassandra host", value=CASSANDRA_HOST, key="c_host")
        st.caption(f"Keyspace: **{KEYSPACE}**")
        if st.button("Recargar datos"):
            _cluster_cassandra.clear()
            st.rerun()
        st.divider()
        st.markdown("**UIs (Airflow, NiFi)**")
        ui_host = _get_default_ui_host()
        st.markdown(
            f"• [Airflow](http://{ui_host}:8080) · "
            f"[NiFi](https://{ui_host}:8443/nifi)"
        )
        st.caption("Airflow: admin/admin · NiFi: ver nifi-app.log")
        st.divider()
        st.markdown("**Pipeline local**")
        st.caption("Ejecuta `producer.py` + Spark (requiere Kafka/HDFS/Cassandra según config).")
        if st.button("Ejecutar ciclo 15 min", type="primary", use_container_width=True):
            ejecutar_ciclo_pipeline()

    if "paso_15min" not in st.session_state:
        st.session_state["paso_15min"] = 0

    session = obtener_session_cassandra(host)
    modo_demo = session is None

    # =========================
    # Demostración KDD (paso a paso)
    # =========================
    st.subheader("Demostración KDD (fases y verificación)")
    st.caption(
        "**Orden:** pestaña **0** (levantar → comprobar → parar) → **1** → **2** → **3**. "
        "Abajo, **Monitorización** agrupa enlaces y exploradores. El **mapa** está al final de la página."
    )
    tab0, tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📊 Cuadro de mando",
            "0 · Entorno (servicios)",
            "1 · Ingesta",
            "2 · Procesamiento",
            "3 · Validación",
        ]
    )

    with tab0:
        _render_cuadro_mando_directivo()

    with tab1:
        st.markdown("### Fase 0 — Entorno (orden fijo)")
        st.caption(
            "**1 → Arrancar todo** (HDFS, Kafka, Cassandra, topics Kafka, esquema Cassandra, esquema Hive si hay CLI). "
            "**2 → Comprobar**. **3 → Parar** solo al cerrar la demo. "
            "Abajo: **Monitorización** para enlaces y consultas sin perder el orden."
        )
        st.markdown(
            "Qué incluye el arranque automático: **HDFS** → **Kafka** → **Cassandra** → **topics** → "
            f"**keyspace `{KEYSPACE}`** → **Hive `{HIVE_DB}`** (`setup_hive.hql`, si HDFS + `spark-sql`/`hive` están disponibles)."
        )
        st.info(
            "**Equivalente en terminal:** `cd ~/smart_energy && ./scripts/iniciar_servicios.sh` "
            "(servicios base) y, si hace falta, vuelve a pulsar **Arrancar** en el dashboard para topics + esquemas."
        )

        st.markdown("##### Paso 1 — Arrancar")
        if st.button("▶ Arrancar servicios (completo)", type="primary", use_container_width=True, key="fase0_btn_arrancar"):
            with st.spinner("Arrancando HDFS → Kafka → Cassandra → topics → esquemas..."):
                res = _fase0_arrancar_servicios()
            st.session_state["fase0_check"] = res.get("check_final", {})
            st.success("Arranque lanzado. Comprobación al vuelo:")
            st.json(res.get("check_final", {}))
            with st.expander("Detalle técnico del arranque"):
                st.json({k: v for k, v in res.items() if k != "check_final"})
                _cf = res.get("check_final") or {}
                if _cf.get("hive_catalog_hint"):
                    st.caption("Hive — aviso del chequeo:")
                    st.info(_cf["hive_catalog_hint"])

        st.markdown("##### Paso 2 — Comprobar")
        if st.button("✓ Comprobar servicios", use_container_width=True, key="fase0_btn_comprobar"):
            with st.spinner("Comprobando HDFS, Kafka, Cassandra, topics, Hive..."):
                res = _comprobar_servicios_base()
            st.session_state["fase0_check"] = res
            st.json(res)

        st.markdown("##### Paso 3 — Parar (al terminar)")
        if st.button("■ Parar servicios base", use_container_width=True, key="fase0_btn_parar"):
            with st.spinner("Parando..."):
                res = _fase0_parar_servicios()
            st.warning("Servicios parados (o ya estaban parados). Estado:")
            st.json(res.get("check_final", {}))

        _chk = st.session_state.get("fase0_check") or {}
        if _chk:
            c0, c1, c2, c3 = st.columns(4)
            c0.metric("HDFS", "✓" if _chk.get("hdfs_ok") else "✗")
            c1.metric("Kafka:9092", "✓" if _chk.get("kafka_ok") else "✗")
            c2.metric("Cassandra", "✓" if _chk.get("cassandra_ok") else "✗")
            c3.metric("Keyspace", "✓" if _chk.get("keyspace_ok") else "✗")
            c4, c5 = st.columns(2)
            c4.metric("Topics Kafka", "✓" if _chk.get("topics_ok") else "✗")
            c5.metric("Catálogo Hive", "✓" if _chk.get("hive_catalog_ok") else ("—" if not _chk.get("hive_cli_available") else "✗"))

        if not _chk.get("cassandra_ok", True):
            with st.expander("Si Cassandra no arranca — qué hacer", expanded=True):
                st.markdown(
                    """
1. **Arranque manual**: pestaña **Monitorización** → **Arrancar Cassandra**, o terminal: `./iniciar_servicios.sh --only cassandra`.
2. **Java / JVM**: `./scripts/patch_cassandra_java21_jvm.sh` si ves `UseBiasedLocking` en el log.
3. **Puerto**: `nc -z 127.0.0.1 9042` · Log: `cassandra/logs/` o `/tmp/smart_grid_cassandra.log`.
4. **Driver Python**: `pip install cassandra-driver` en el venv de Streamlit.
                    """
                )
        if _chk and not _chk.get("keyspace_ok", True) and _chk.get("cassandra_ok"):
            st.info(
                "Cassandra en 9042 pero **keyspace** no listo. "
                "`./scripts/aplicar_esquema_cassandra.sh` o **Arrancar** otra vez en Fase 0."
            )
        if _chk and _chk.get("hive_cli_available") and not _chk.get("hive_catalog_ok"):
            _hh = (_chk.get("hive_catalog_hint") or "").strip()
            st.warning(
                _hh
                if _hh
                else (
                    f"Hive CLI disponible pero la base **`{HIVE_DB}`** no aparece en SHOW DATABASES. "
                    "Comprueba HDFS + `./scripts/instalar_hive_java21.sh`, o ejecuta **`setup_hive.hql`** / **Arrancar** en Fase 0."
                )
            )
            st.caption(
                f"Base esperada: **`{HIVE_DB}`** (config `HIVE_DB`). "
                "Si el metastore aún no tiene tablas, aplica **`setup_hive.hql`** o **Fase 0 → Arrancar** cuando Hive/Spark estén listos."
            )

    with tab2:
        st.markdown("### Qué haces")
        st.write(
            "Ejecutas `producer.py` para generar una tanda sintética/real de la red: "
            "publica lecturas en Kafka (`energy_raw` y `weather_raw`) y escribe backup `energy_*.json` en HDFS."
        )
        colA, colB = st.columns(2)
        with colA:
            if st.button("Arrancar ingesta (producer.py)", type="primary", use_container_width=True):
                with st.spinner("Ejecutando producer.py..."):
                    rc, out = _fase1_ejecutar_producer()
                if rc == 0:
                    st.success("producer.py finalizó correctamente.")
                else:
                    st.error("producer.py falló.")
                with st.expander("Salida producer.py"):
                    st.code(out[:8000])
        with colB:
            if st.button("Comprobar ingesta", use_container_width=True):
                with st.spinner("Verificando HDFS + Kafka..."):
                    hdfs_res = _verificar_hdfs_backup()
                    kafka_res = _verificar_kafka_mensajes(KAFKA_BOOTSTRAP)
                st.subheader("HDFS")
                st.json(hdfs_res)
                st.subheader("Kafka")
                st.json(kafka_res)

        st.divider()
        with st.expander("NiFi (Fase I): botones y procesadores"):
            st.markdown("### Botones")
            colN1, colN2, colN3 = st.columns(3)
            with colN1:
                if st.button("Arrancar NiFi", type="primary", use_container_width=True):
                    with st.spinner("Arrancando NiFi..."):
                        ok, msg = _nifi_start()
                    if ok:
                        st.success("NiFi arrancó y responde en el puerto esperado.")
                    else:
                        st.error("NiFi no arrancó o aún no responde. Revisa la salida.")
                    st.code(msg[:6000] or "—")
            with colN2:
                if st.button("Comprobar NiFi", use_container_width=True):
                    res = _nifi_status()
                    st.json(res)
            with colN3:
                if st.button("Parar NiFi", use_container_width=True):
                    with st.spinner("Parando NiFi..."):
                        ok, msg = _nifi_stop()
                    if ok:
                        st.warning("NiFi detenido.")
                    else:
                        st.error("No se pudo detener NiFi o todavía está levantado. Revisa la salida.")
                    st.code(msg[:6000] or "—")

            st.markdown("### Procesadores que necesitas (Fase I)")
            st.write(
                "Esta lista es la implementación típica para cumplir el PDF: consumir APIs (OpenWeather/ElectricityMaps), transformar y publicar en Kafka (`energy_raw` y `weather_raw`)."
            )
            req = _nifi_required_processors_fase1()
            st.dataframe(req, use_container_width=True, hide_index=True)

            st.markdown("### Procesadores actualmente en tu NiFi")
            activos = _nifi_flow_processors_activos()
            if not activos:
                st.info(
                    "Tu `flow.json.gz` de NiFi está vacío (0 procesadores). "
                    "Para ver procesadores reales en ejecución, hay que importar/crear el flujo NiFi de ingesta."
                )
            else:
                st.write(f"Encontrados {len(activos)} procesadores en el flow actual:")
                st.code("\n".join(activos[:120]))

            st.divider()
            if st.button("Crear procesadores NiFi (Fase I demo)", type="secondary", use_container_width=True):
                with st.spinner("Creando procesadores en NiFi via REST..."):
                    res = _nifi_crear_procesadores_fase1_demo()
                if res.get("ok"):
                    st.success("Procesadores NiFi creados.")
                    if res.get("created"):
                        st.subheader("Creados")
                        st.json(res["created"])
                    if res.get("skipped"):
                        st.subheader("Ya existían")
                        st.write(res["skipped"])
                else:
                    st.error("Falló la creación de procesadores en NiFi.")
                    st.code(str(res)[:6000])

            st.markdown("### Conectar y ejecutar")
            st.caption("Tras crear los procesadores, conéctalos entre sí (wiring), configura Kafka Connection Service y arranca/para la ejecución.")
            colC1, colC2, colC3 = st.columns(3)
            with colC1:
                if st.button("Conectar procesadores (Fase I)", type="primary", use_container_width=True):
                    with st.spinner("Conectando y configurando procesadores..."):
                        res = _nifi_conectar_y_configurar_fase1()
                    if res.get("ok"):
                        st.success(res.get("message", "Procesadores conectados."))
                    else:
                        st.error(res.get("error", "Error."))
                        st.code(str(res)[:3000])
            with colC2:
                if st.button("Arrancar procesadores", use_container_width=True):
                    with st.spinner("Poniendo procesadores en RUNNING..."):
                        res = _nifi_arrancar_procesadores_fase1()
                    if res.get("ok"):
                        st.success(res.get("message", "Procesadores en ejecución."))
                    else:
                        st.error(res.get("error", "Error."))
                        st.code(str(res)[:3000])
            with colC3:
                if st.button("Parar procesadores", use_container_width=True):
                    with st.spinner("Parando procesadores..."):
                        res = _nifi_parar_procesadores_fase1()
                    if res.get("ok"):
                        st.warning(res.get("message", "Procesadores parados."))
                    else:
                        st.error(res.get("error", "Error."))
                        st.code(str(res)[:3000])

    with tab3:
        st.markdown("### Qué haces (KDD: selección / transformación / carga)")
        st.write(
            "Ejecutas Spark con GraphFrames (`procesamiento_grafos.py`): grafo de la red, autosanación, **PageRank**, "
            "puntos de fallo. **Carga**: resultados en **Cassandra** (tiempo real) y en **Hive** "
            f"`{HIVE_DB}` (histórico: `subestaciones_historico`, `lineas_historico`, `metricas_subestaciones_hist`, …) vía `persistencia_hive`."
        )
        st.info(
            "Verifica tablas Hive en **Monitorización → consultas por capa (Fase 2: Hive)**. "
            "CLI: `spark-sql` o `hive` con Java 21 + Hive 4.2."
        )
        colA, colB = st.columns(2)
        with colA:
            if st.button("Arrancar procesamiento (Spark)", type="primary", use_container_width=True):
                with st.spinner("Ejecutando Spark (procesamiento_grafos.py)..."):
                    rc, out = _fase2_ejecutar_spark()
                if rc == 0:
                    st.success("Spark finalizó correctamente.")
                else:
                    st.error("Spark falló (revisa Cassandra en 9042 y keyspace).")
                with st.expander("Salida Spark"):
                    st.code(out[:12000])
                # Fuerza refresco de la conexión Cassandra cacheada.
                _cluster_cassandra.clear()
                st.rerun()
        with colB:
            if st.button("Comprobar persistencia en Cassandra", use_container_width=True):
                with st.spinner("Consultando Cassandra..."):
                    res = _fase2_verificar_cassandra()
                st.json(res)

    with tab4:
        st.markdown("### Qué haces")
        st.write(
            "Validación final del ciclo KDD: comprobar **Hive** (histórico / reporting) y **Cassandra** (estado actual), "
            "y que el dashboard refleje telemetría. Usa **Monitorización → consultas por capa** (Hive + Cassandra). "
            "Si Cassandra está vacía, verás modo demo."
        )
        colA, colB = st.columns(2)
        with colA:
            if st.button("Comprobar Cassandra (conteos)", use_container_width=True):
                res = _fase2_verificar_cassandra()
                st.json(res)
        with colB:
            if st.button("Recargar datos (dashboard)", use_container_width=True):
                _cluster_cassandra.clear()
                st.rerun()

    with st.expander(
        "🛠 Monitorización y herramientas (enlaces ordenados, Hive, Kafka, consultas KDD)",
        expanded=False,
    ):
        import importlib
        import sys

        _kdd = importlib.import_module("app_visualizacion_kdd_panel")
        _kdd.render_kdd_tools_panel(sys.modules[__name__], host)

    if modo_demo:
        _h = (host or CASSANDRA_HOST).strip()
        if Cluster is None:
            _msg_demo = (
                "**cassandra-driver** no está instalado en el venv de Streamlit → `pip install cassandra-driver`. "
                f"En Fase 0, `cassandra_ok` puede ser true (puerto 9042) pero **keyspace_ok** y el mapa siguen fallando sin driver."
            )
        elif _port_open("127.0.0.1", 9042) and not _cassandra_keyspace_exists(_h):
            _msg_demo = (
                "**Cassandra está levantada** (puerto 9042 abierto; eso es `cassandra_ok: true`), "
                f"pero **keyspace_ok: false** = no existe el keyspace **`{KEYSPACE}`** (o el driver no pudo listarlo). "
                f"Ejecuta: `./cassandra/bin/cqlsh -f cassandra/esquema_smart_grid.cql` o `./scripts/aplicar_esquema_cassandra.sh`, o Fase 0 hasta aplicar el esquema."
            )
        else:
            _msg_demo = (
                f"No hay sesión al keyspace **{KEYSPACE}** en **{_h}** (revisa host en la barra lateral, firewall o credenciales). "
                "Si el servicio no está en 9042, arranca Cassandra primero."
            )
        st.warning(
            _msg_demo
            + " Mapa en **modo topología** (demo). Con datos: `producer.py` + `procesamiento_grafos.py`."
        )
        subestaciones = datos_demo()
        lineas = {}
        pagerank = {}
        articulaciones = []
    else:
        subestaciones = cargar_subestaciones(session)
        lineas = cargar_lineas(session)
        pagerank = cargar_pagerank(session)
        articulaciones = cargar_puntos_fallo(session)
        if not subestaciones:
            st.info("Cassandra conectada pero **subestaciones_estado** vacía. Ejecuta un ciclo del pipeline.")
            subestaciones = datos_demo()
            modo_demo = True

    # KPIs
    n_alert = sum(1 for s in subestaciones.values() if s.get("estado") == "alerta")
    n_sobre = sum(1 for s in subestaciones.values() if s.get("estado") == "sobrecarga")
    pot_total = sum(float(s.get("potencia_mw") or 0) for s in subestaciones.values())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Subestaciones", len(subestaciones))
    c2.metric("Potencia total (MW)", f"{pot_total:,.0f}")
    c3.metric("Alertas", n_alert, delta=None if n_alert == 0 else "revisar")
    c4.metric("Sobrecargas", n_sobre, delta=None if n_sobre == 0 else "crítico")

    st.caption(
        f"Paso simulación: **{st.session_state['paso_15min']}** · "
        "El mapa se actualiza tras **Ejecutar ciclo 15 min** (producer + Spark) o **Recargar datos**."
    )

    # Informe de cambios entre ciclos (cuando hay datos reales y previo)
    if not modo_demo and subestaciones:
        _prev = st.session_state.get("prev_cycle_snapshot")
        _snap = {
            "subestaciones": {k: {"estado": v.get("estado"), "potencia_mw": float(v.get("potencia_mw") or 0)} for k, v in subestaciones.items()},
            "pagerank": dict(pagerank),
            "articulacion_ids": {a["id"] for a in articulaciones},
            "n_alert": n_alert,
            "n_sobre": n_sobre,
            "pot_total": pot_total,
        }
        if _prev:
            _informe = _generar_informe_cambios_ciclo(_prev, _snap)
            with st.expander("📋 Cambios desde el ciclo anterior", expanded=_informe["hay_cambios"]):
                st.markdown(_informe["texto"])
                if _informe.get("cambios_estado"):
                    st.dataframe(_informe["cambios_estado"], use_container_width=True, hide_index=True)
        st.session_state["prev_cycle_snapshot"] = _snap

    mapa = construir_mapa(subestaciones, lineas, pagerank)
    st_folium(mapa, width=None, height=520, returned_objects=[])

    st.subheader("Leyenda")
    lc1, lc2, lc3 = st.columns(3)
    lc1.markdown("Verde: **OK**")
    lc2.markdown("Naranja: **Alerta**")
    lc3.markdown("Rojo: **Sobrecarga**")

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("PageRank (nodos críticos)")
        if pagerank:
            top = sorted(pagerank.items(), key=lambda x: -x[1])[:12]
            st.dataframe(
                [{"Subestación": n, "PageRank": round(v, 5)} for n, v in top],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("Sin datos de PageRank en Cassandra.")

    with col_b:
        st.subheader("Puntos de fallo únicos (articulación)")
        if articulaciones:
            st.dataframe(
                [
                    {"Subestación": a["id"], "Fragmentos si cae": a["fragmentos"]}
                    for a in articulaciones[:15]
                ],
                use_container_width=True,
                hide_index=True,
            )
            with st.expander("Detalle JSON (fragmentos aislados)"):
                for a in articulaciones[:5]:
                    st.markdown(f"**{a['id']}**")
                    st.code(a["detalle"] or "—")
        else:
            st.info(
                "Ningún nodo de articulación en la última ejecución, o tabla vacía. "
                "Tras procesar el grafo conexo, los hubs suelen ser críticos."
            )

    st.divider()
    st.caption(
        "Streamlit + Folium · Datos: Cassandra `smart_grid` · "
        "Ejecutar: `streamlit run app_visualizacion.py`"
    )


if __name__ == "__main__":
    main()
