#!/usr/bin/env python3
"""
Comprueba flujo NiFi: crea procesadores, conecta, arranca, ejecuta ingesta,
y verifica provenance, colas y destinos.
Uso: API_WEATHER_KEY=xxx python scripts/nifi_flujo_comprobar.py
"""
import copy
import json
import os
import sys
import time
import urllib.request
import urllib.error
import ssl
import subprocess
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
NIFI_URL = "https://localhost:8443/nifi-api"
NIFI_USER = os.environ.get("NIFI_USER", "55d7890e-d973-41d7-b24c-8bdf9c440dcb")
NIFI_PASS = os.environ.get("NIFI_PASS", "fIQRYTw9dUQNvHJO1oNHj2FDncuzWJGj")
API_KEY = os.environ.get("API_WEATHER_KEY", "")

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def _req(method, path, data=None, token=None):
    url = f"{NIFI_URL}{path}"
    hdr = {"Content-Type": "application/json"}
    if token:
        hdr["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(url, data=json.dumps(data).encode() if data else None, headers=hdr, method=method)
    with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
        return json.loads(r.read().decode())


def _get_token():
    url = f"{NIFI_URL}/access/token"
    data = f"username={NIFI_USER}&password={NIFI_PASS}".encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/x-www-form-urlencoded"}, method="POST")
    with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
        return r.read().decode().strip()


def _arrancar_procesadores(token, procs):
    """Pone en RUNNING los procesadores que están STOPPED."""
    started = []
    for p in procs:
        comp = p.get("component", {})
        pid = comp.get("id")
        name = comp.get("name", "?")
        if comp.get("state") != "RUNNING":
            try:
                rev = p.get("revision", {})
                body = {"state": "RUNNING", "revision": rev}
                req = urllib.request.Request(
                    f"{NIFI_URL}/processors/{pid}/run-status",
                    data=json.dumps(body).encode(),
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    method="PUT",
                )
                with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
                    if r.status in (200, 202):
                        started.append(name)
            except Exception as e:
                print(f"  No se pudo arrancar {name}: {e}")
    return started


def main():
    do_start = "--start" in sys.argv or "-s" in sys.argv
    print("=== NiFi: comprobación flujo, provenance, colas, destinos ===\n")
    try:
        token = _get_token()
    except Exception as e:
        print(f"ERROR: No se pudo obtener token NiFi: {e}")
        return 1

    try:
        root = _req("GET", "/flow/process-groups/root", token=token)
        pg_id = root["processGroupFlow"]["id"]
        flow = root["processGroupFlow"].get("flow", {})
        procs = flow.get("processors", [])
        conns = flow.get("connections", [])
    except Exception as e:
        print(f"ERROR obteniendo flujo: {e}")
        return 1

    print("--- 1. FLUJO ACTUAL ---")
    print(f"Root ID: {pg_id}")
    print(f"Procesadores: {len(procs)}")
    for p in procs:
        comp = p.get("component", {})
        st = comp.get("state", "?")
        print(f"  - {comp.get('name')} [{st}]")
    stopped_procs = [p.get("component", {}).get("name") for p in procs if p.get("component", {}).get("state") == "STOPPED"]
    print(f"Conexiones: {len(conns)}")
    for c in conns:
        src = c.get("source", {}).get("name", "?")
        dst = c.get("destination", {}).get("name", "?")
        snap = c.get("status", {}).get("aggregateSnapshot", {})
        q = snap.get("queuedCount", 0)
        qs = snap.get("queued", "0")
        print(f"  {src} -> {dst} | cola: {q} ff, {qs}")
    print()

    if do_start and procs:
        print("--- Arrancando procesadores STOPPED ---")
        started = _arrancar_procesadores(token, procs)
        if started:
            print(f"Arrancados: {', '.join(started)}")
            time.sleep(2)
        print()

    print("--- 2. PROVENANCE (últimos eventos) ---")
    try:
        # API asíncrona: POST crea búsqueda, poll GET hasta terminar, DELETE al final
        prov = _req("POST", "/provenance", {"provenance": {"request": {"maxResults": 50}}}, token=token)
        prov_id = (prov.get("provenance") or {}).get("id")
        events = []
        if prov_id:
            for _ in range(15):
                time.sleep(1)
                p = _req("GET", f"/provenance/{prov_id}", token=token)
                prov_obj = p.get("provenance", p)
                if prov_obj.get("finished"):
                    results = prov_obj.get("results") or {}
                    events = results.get("provenanceEvents") or []
                    break
            try:
                _req("DELETE", f"/provenance/{prov_id}", token=token)
            except Exception:
                pass
        print(f"Eventos totales: {len(events)}")
        for e in events[:15]:
            print(f"  {e.get('eventType')} | {e.get('componentName')} | uuid:{str(e.get('flowFileUuid',''))[:12]}... | {e.get('transitUri','')[:60]}")
    except urllib.error.HTTPError as e:
        print(f"  Provenance API: {e.code} (flujo vacío o formato búsqueda distinto)")
    except Exception as e:
        print(f"  {e}")
    print()

    print("--- 3. COLAS (estado conexiones) ---")
    try:
        st = _req("GET", f"/flow/process-groups/{pg_id}/status", token=token)
        agg = st.get("processGroupStatus", {}).get("aggregateSnapshot", {})
        conn_status = agg.get("connectionStatusSnapshots", [])
        for cs in conn_status[:20]:
            name = cs.get("name", "?")
            q = cs.get("queuedCount", 0)
            qs = cs.get("queued", "0")
            if q > 0 or "->" in str(name):
                print(f"  {name} | cola: {q} ff, {qs}")
    except Exception as e:
        print(f"  {e}")
    print()

    print("--- 4. DESTINOS (HDFS, Kafka) ---")
    try:
        r = subprocess.run(["hdfs", "dfs", "-ls", "/user/hadoop/energy_backup"], capture_output=True, text=True, timeout=20)
    except subprocess.TimeoutExpired:
        class R: returncode = -1; stdout = ""
        r = R()
    if r.returncode == 0:
        lines = [l for l in (r.stdout or "").splitlines() if "energy_" in l or "weather" in l]
        print(f"HDFS /user/hadoop/energy_backup: {len(lines)} ficheros")
        for l in lines[-8:]:
            parts = l.split()
            print(f"  {parts[-1] if parts else l}")
    else:
        print("HDFS: no disponible o sin datos")
    nc_r = subprocess.run(["nc", "-z", "127.0.0.1", "9092"], capture_output=True)
    kafka_ok = nc_r.returncode == 0
    kafka_topics = ""
    if kafka_ok:
        try:
            kafka_home = os.environ.get("KAFKA_HOME", "/opt/kafka")
            topics_out = subprocess.run(
                [str(Path(kafka_home) / "bin" / "kafka-topics.sh"), "--bootstrap-server", "localhost:9092", "--list"],
                capture_output=True, text=True, timeout=10,
            )
            if topics_out.returncode == 0 and topics_out.stdout:
                kafka_topics = " | topics: " + ", ".join(topics_out.stdout.strip().split("\n")[:8])
        except Exception:
            pass
    print("Kafka:", "OK (puerto 9092)" + kafka_topics if kafka_ok else "No activo (./iniciar_servicios)")
    print()

    if stopped_procs and not do_start:
        print("--- NOTA ---")
        print(f"Procesadores STOPPED: {', '.join(stopped_procs)}. Para arrancarlos: python scripts/nifi_flujo_comprobar.py --start")
        print()

    if len(procs) == 0:
        print("--- NOTA: Flujo vacío ---")
        print("No hay procesadores. Para crear: importar nifi/smart_grid_flow_definition.json desde la UI,")
        print("o usar el dashboard Fase 1 (requiere que app use auth).")
        print("Ejecutando producer.py para verificar ingesta HDFS...")
        env = os.environ.copy()
        env["API_WEATHER_KEY"] = API_KEY or env.get("API_WEATHER_KEY", "")
        rr = subprocess.run(
            [sys.executable, str(BASE / "producer.py")],
            cwd=str(BASE),
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        print(rr.stdout or "")
        if rr.stderr:
            print("STDERR:", rr.stderr[:300])
        print("Return:", rr.returncode)

    return 0


if __name__ == "__main__":
    sys.exit(main())
