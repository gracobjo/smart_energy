#!/usr/bin/env python3
"""
Genera informe consolidado de todas las fases KDD del pipeline Smart Grid.
Recopila: servicios, HDFS, Kafka, Cassandra, Hive, NiFi, estado del pipeline.
Uso: python scripts/generar_informe_fases.py [--output /ruta/informe.md]
"""
import argparse
import json
import os
import socket
import subprocess
import sys
from datetime import datetime
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except Exception:
        return False


def _run(cmd, timeout=15):
    try:
        r = subprocess.run(
            cmd if isinstance(cmd, list) else cmd.split(),
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(BASE),
        )
        return r.returncode == 0, (r.stdout or "") + (r.stderr or "")
    except Exception as e:
        return False, str(e)


def fase0_servicios() -> dict:
    """Estado de servicios base."""
    return {
        "hdfs": _port_open("127.0.0.1", 9870) or _port_open("127.0.0.1", 9000),
        "kafka": _port_open("127.0.0.1", 9092),
        "cassandra": _port_open("127.0.0.1", 9042),
        "nifi": _port_open("127.0.0.1", 8443),
        "airflow": _port_open("127.0.0.1", 8080),
    }


def fase1_ingesta() -> dict:
    """HDFS energy_backup, Kafka topics."""
    out = {"hdfs_files": 0, "hdfs_paths": [], "kafka_topics": []}
    ok, text = _run(["hdfs", "dfs", "-ls", "/user/hadoop/energy_backup"], timeout=10)
    if ok and text:
        lines = [l for l in text.splitlines() if "energy_" in l or "weather" in l]
        out["hdfs_files"] = len(lines)
        out["hdfs_paths"] = [l.split()[-1] for l in lines[-5:] if len(l.split()) >= 8]
    kafka_home = os.environ.get("KAFKA_HOME", "/opt/kafka")
    topics_sh = Path(kafka_home) / "bin" / "kafka-topics.sh"
    if topics_sh.exists():
        ok, text = _run([str(topics_sh), "--bootstrap-server", "localhost:9092", "--list"], timeout=10)
        if ok and text:
            out["kafka_topics"] = [t.strip() for t in text.strip().splitlines() if t.strip()]
    return out


def fase2_procesamiento() -> dict:
    """Cassandra: subestaciones, líneas, PageRank."""
    out = {"cassandra_tables": {}, "keyspace_ok": False}
    if not _port_open("127.0.0.1", 9042):
        out["error"] = "Cassandra no responde en 9042"
        return out
    try:
        from cassandra.cluster import Cluster
        cluster = Cluster(["127.0.0.1"], port=9042, connect_timeout=5)
        session = cluster.connect("smart_grid")
        for table in ["subestaciones_estado", "lineas_estado", "pagerank_subestaciones", "puntos_fallo_unicos"]:
            try:
                rows = session.execute(f"SELECT COUNT(*) FROM {table}")
                out["cassandra_tables"][table] = list(rows)[0][0] if rows else 0
            except Exception:
                out["cassandra_tables"][table] = "error"
        out["keyspace_ok"] = True
        cluster.shutdown()
    except ImportError:
        out["error"] = "cassandra-driver no instalado (pip install cassandra-driver)"
    except Exception as e:
        out["error"] = str(e)
    return out


def fase3_hive(quick: bool = False) -> dict:
    """Hive: bases de datos, tablas smart_grid_analytics."""
    out = {"databases": [], "tables": [], "ok": False}
    if quick:
        return out
    for exe in ["spark-sql", "hive"]:
        ok, text = _run([exe, "-e", "SHOW DATABASES;"], timeout=10)
        if ok and ("smart_grid" in (text or "").lower() or "default" in (text or "").lower()):
            out["ok"] = True
            out["databases"] = [l.strip() for l in (text or "").splitlines() if l.strip() and not l.startswith("-")][:10]
            break
    return out


def fase_nifi(quick: bool = False) -> dict:
    """Estado flujo NiFi (puerto + opcional flujo)."""
    out = {"puerto_ok": _port_open("127.0.0.1", 8443), "procesadores": 0, "conexiones": 0, "ok": False}
    if not out["puerto_ok"] or quick:
        return out
    try:
        ok, text = _run([sys.executable, str(BASE / "scripts" / "nifi_flujo_comprobar.py")], timeout=12)
        if ok:
            out["ok"] = True
            for line in (text or "").splitlines():
                if "Procesadores:" in line:
                    try:
                        out["procesadores"] = int(line.split(":")[-1].strip())
                    except Exception:
                        pass
                elif "Conexiones:" in line:
                    try:
                        out["conexiones"] = int(line.split(":")[-1].strip())
                    except Exception:
                        pass
    except Exception as e:
        out["error"] = str(e)
    return out


def generar_markdown(report: dict) -> str:
    """Genera informe en Markdown."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        "# Informe pipeline Smart Grid — Todas las fases KDD",
        f"**Generado:** {ts}",
        "",
        "## Fase 0 — Servicios",
        "",
        "| Servicio | Estado |",
        "|----------|--------|",
    ]
    for svc, ok in report.get("fase0_servicios", {}).items():
        lines.append(f"| {svc} | {'✓ OK' if ok else '✗ No'} |")
    lines.extend(["", "## Fase I — Ingesta", ""])
    fi = report.get("fase1_ingesta", {})
    lines.append(f"- HDFS energy_backup: **{fi.get('hdfs_files', 0)}** ficheros")
    lines.append(f"- Kafka topics: {', '.join(fi.get('kafka_topics', [])[:10]) or '—'}")
    if fi.get("hdfs_paths"):
        lines.append("- Últimos ficheros HDFS:")
        for p in fi["hdfs_paths"][-3:]:
            lines.append(f"  - `{p}`")
    lines.extend(["", "## Fase II — Procesamiento (Cassandra)", ""])
    f2 = report.get("fase2_procesamiento", {})
    if f2.get("keyspace_ok"):
        for tbl, cnt in f2.get("cassandra_tables", {}).items():
            lines.append(f"- **{tbl}**: {cnt} registros")
    else:
        lines.append(f"- Keyspace: {f2.get('error', 'no disponible')}")
    lines.extend(["", "## Fase III — Hive (histórico)", ""])
    f3 = report.get("fase3_hive", {})
    lines.append(f"- Hive catálogo: {'✓ OK' if f3.get('ok') else '✗ No disponible'}")
    if f3.get("databases"):
        lines.append(f"- Bases: {', '.join(f3['databases'][:8])}")
    lines.extend(["", "## NiFi — Flujo ingesta", ""])
    fn = report.get("fase_nifi", {})
    lines.append(f"- Puerto 8443: {'✓ OK' if fn.get('puerto_ok') else '✗ Cerrado'}")
    lines.append(f"- Procesadores: **{fn.get('procesadores', 0)}**")
    lines.append(f"- Conexiones: **{fn.get('conexiones', 0)}**")
    lines.append(f"- Flujo: {'✓ OK' if fn.get('ok') else '✗ ' + str(fn.get('error', 'no comprobado'))}")
    lines.extend(["", "---", f"*Informe generado por `generar_informe_fases.py`*"])
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Genera informe de todas las fases KDD")
    parser.add_argument("--output", "-o", default="", help="Ruta salida (MD o JSON). Por defecto: reports/informe_fases_YYYYMMDD_HHMMSS.md")
    parser.add_argument("--format", "-f", choices=["md", "json"], default="md", help="Formato de salida")
    parser.add_argument("--quick", "-q", action="store_true", help="Solo comprobaciones rápidas (sin Hive/NiFi flujo)")
    args = parser.parse_args()

    quick = args.quick
    report = {
        "timestamp": datetime.now().isoformat(),
        "fase0_servicios": fase0_servicios(),
        "fase1_ingesta": fase1_ingesta(),
        "fase2_procesamiento": fase2_procesamiento(),
        "fase3_hive": fase3_hive(quick=quick),
        "fase_nifi": fase_nifi(quick=quick),
    }

    if args.format == "json":
        content = json.dumps(report, indent=2, default=str)
    else:
        content = generar_markdown(report)

    if args.output:
        out_path = Path(args.output)
    else:
        reports_dir = BASE / "reports"
        reports_dir.mkdir(exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        ext = "json" if args.format == "json" else "md"
        out_path = reports_dir / f"informe_fases_{ts}.{ext}"

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(content, encoding="utf-8")
    print(f"Informe guardado en: {out_path}")
    if args.format == "md":
        print(content[:1500] + ("..." if len(content) > 1500 else ""))
    return 0


if __name__ == "__main__":
    sys.exit(main())
