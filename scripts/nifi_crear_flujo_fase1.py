#!/usr/bin/env python3
"""
Crea procesadores NiFi Fase 1 via API (requiere NIFI_USER, NIFI_PASS en .env o env).
Uso: NIFI_USER=xxx NIFI_PASS=xxx python scripts/nifi_crear_flujo_fase1.py
"""
import os
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

# Cargar .env si existe
env_file = BASE / ".env"
if env_file.exists():
    for line in env_file.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

# Ejecutar la lógica del dashboard
os.environ.setdefault("STREAMLIT_SERVER_HEADLESS", "true")
import requests
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

NIFI_URL = "https://localhost:8443/nifi-api"
USER = os.environ.get("NIFI_USER")
PASS = os.environ.get("NIFI_PASS")
if not USER or not PASS:
    print("ERROR: Definir NIFI_USER y NIFI_PASS (ver nifi-app.log)")
    sys.exit(1)


def auth():
    r = requests.post(f"{NIFI_URL}/access/token",
                      data=f"username={USER}&password={PASS}",
                      headers={"Content-Type": "application/x-www-form-urlencoded"},
                      verify=False, timeout=10)
    if r.status_code not in (200, 201) or not r.text:
        print("ERROR: No se pudo autenticar en NiFi:", r.status_code)
        sys.exit(1)
    return {"Authorization": f"Bearer {r.text.strip()}"}


def main():
    h = auth()
    r = requests.get(f"{NIFI_URL}/flow/process-groups/root", headers=h, verify=False, timeout=10)
    if r.status_code != 200:
        print("ERROR: No se pudo obtener root")
        sys.exit(1)
    pg_id = r.json()["processGroupFlow"]["id"]
    print("Root ID:", pg_id)

    # Crear GenerateFlowFile
    body = {"revision": {"clientId": "script-1", "version": 0}, "component": {
        "type": "org.apache.nifi.processors.standard.GenerateFlowFile",
        "name": "NiFi_F1_GenerateTrigger",
        "position": {"x": 150, "y": 150},
        "config": {"properties": {"generate-ff-custom-text": "trigger"}},
    }}
    r = requests.post(f"{NIFI_URL}/process-groups/{pg_id}/processors", json=body, headers={**h, "Content-Type": "application/json"}, verify=False, timeout=30)
    if r.status_code not in (200, 201):
        print("ERROR crear GenerateFlowFile:", r.status_code, r.text[:200])
        sys.exit(1)
    gen_id = r.json()["id"]
    print("Creado GenerateFlowFile:", gen_id)

    # Crear ExecuteStreamCommand (producer)
    body = {"revision": {"clientId": "script-2", "version": 0}, "component": {
        "type": "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        "name": "NiFi_F1_ExecuteProducer",
        "position": {"x": 450, "y": 150},
        "config": {"properties": {
            "Command Path": "python3",
            "Command Arguments": "producer.py",
            "Working Directory": str(BASE),
        }},
    }}
    r = requests.post(f"{NIFI_URL}/process-groups/{pg_id}/processors", json=body, headers={**h, "Content-Type": "application/json"}, verify=False, timeout=30)
    if r.status_code not in (200, 201):
        print("ERROR crear ExecuteProducer:", r.status_code)
        sys.exit(1)
    exec_id = r.json()["id"]
    print("Creado ExecuteProducer:", exec_id)

    # Crear conexión
    body = {"revision": {"clientId": "script-3", "version": 0}, "component": {
        "source": {"id": gen_id, "groupId": pg_id, "type": "PROCESSOR"},
        "destination": {"id": exec_id, "groupId": pg_id, "type": "PROCESSOR"},
        "selectedRelationships": ["success"],
    }}
    r = requests.post(f"{NIFI_URL}/process-groups/{pg_id}/connections", json=body, headers={**h, "Content-Type": "application/json"}, verify=False, timeout=30)
    if r.status_code not in (200, 201):
        print("ERROR crear conexión:", r.status_code)
    else:
        print("Conexión creada")

    print("\nOK. Crear Kafka CS y PublishKafka manualmente desde la UI, o usar dashboard Fase 1.")
    print("Luego: Arrancar procesadores y comprobar con scripts/nifi_flujo_comprobar.py")


if __name__ == "__main__":
    main()
