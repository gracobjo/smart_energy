#!/usr/bin/env python3
"""
Smart Grid - Productor Python (ingesta KDD)
- Consume Electricity Maps (intensidad de carbono / mix) para modular la demanda simulada.
- Kafka energy_raw: lecturas de carga (MW) y voltaje (kV) por subestación (+ estado líneas).
- Kafka weather_raw: condiciones climáticas en zonas con plantas solares/eólicas (OpenWeather).
"""
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path
import requests
from datetime import datetime
from typing import Dict, List, Any, Tuple

from config_nodos import get_nodos, get_aristas, SUBESTACIONES_PRINCIPALES
from config_plantas_renovables import ZONAS_RENOVABLES
from config import (
    KAFKA_BOOTSTRAP,
    TOPIC_RAW,
    TOPIC_WEATHER_RAW,
    HDFS_BACKUP_PATH,
    ELECTRICITY_MAPS_API_KEY,
    ELECTRICITY_MAPS_ZONE,
    API_WEATHER_KEY,
    API_WEATHER_BASE,
)


def obtener_electricity_maps() -> Dict[str, Any]:
    """
    Electricity Maps API v3 (zona España por defecto).
    Sin API key: retorno sintético coherente para desarrollo.
    """
    zone = ELECTRICITY_MAPS_ZONE
    if not ELECTRICITY_MAPS_API_KEY:
        hora = datetime.now().hour
        # Simula mayor intensidad en punta diurna (más fósil / menos renovable relativo)
        base_ci = 180 + (40 if 9 <= hora <= 21 else -30)
        return {
            "zone": zone,
            "carbon_intensity_g_co2_kwh": float(base_ci + random.uniform(-25, 25)),
            "renewable_pct": float(max(15, 55 - (hora % 12) * 2 + random.uniform(-5, 10))),
            "fossil_pct": None,
            "nuclear_pct": None,
            "fuente": "sintetico_sin_api_key",
            "timestamp": datetime.now().isoformat(),
        }
    headers = {"auth-token": ELECTRICITY_MAPS_API_KEY}
    out = {
        "zone": zone,
        "carbon_intensity_g_co2_kwh": None,
        "renewable_pct": None,
        "fossil_pct": None,
        "nuclear_pct": None,
        "fuente": "electricity_maps",
        "timestamp": datetime.now().isoformat(),
    }
    try:
        r = requests.get(
            f"https://api.electricitymaps.com/v3/carbon-intensity/latest",
            params={"zone": zone},
            headers=headers,
            timeout=15,
        )
        if r.ok:
            data = r.json()
            out["carbon_intensity_g_co2_kwh"] = float(data.get("carbonIntensity") or data.get("data", {}).get("carbonIntensity") or 0)
        r2 = requests.get(
            f"https://api.electricitymaps.com/v3/power-breakdown/latest",
            params={"zone": zone},
            headers=headers,
            timeout=15,
        )
        if r2.ok:
            pb = r2.json()
            power = pb.get("powerConsumptionBreakdown") or pb.get("data", {}).get("powerConsumptionBreakdown") or {}
            total = sum(float(v or 0) for v in power.values()) or 1.0
            ren = sum(float(power.get(k, 0) or 0) for k in ("wind", "solar", "hydro", "biomass", "geothermal"))
            out["renewable_pct"] = round(100.0 * ren / total, 2)
            fos = float(power.get("fossil", 0) or 0) + float(power.get("gas", 0) or 0) + float(power.get("coal", 0) or 0)
            out["fossil_pct"] = round(100.0 * fos / total, 2)
            out["nuclear_pct"] = round(100.0 * float(power.get("nuclear", 0) or 0) / total, 2)
    except Exception as e:
        out["error"] = str(e)
        out["carbon_intensity_g_co2_kwh"] = 200.0
        out["renewable_pct"] = 40.0
    if out.get("carbon_intensity_g_co2_kwh") is None:
        out["carbon_intensity_g_co2_kwh"] = 200.0
    if out.get("renewable_pct") is None:
        out["renewable_pct"] = 35.0
    return out


def _factor_demanda_desde_em(em: Dict, hora: int) -> float:
    """Mayor estrés de red cuando baja renovable o sube carbono (proxy demanda no cubierta)."""
    ren = float(em.get("renewable_pct") or 40)
    ci = float(em.get("carbon_intensity_g_co2_kwh") or 200)
    # Menos renovable -> factor de carga algo mayor
    stress = (100 - min(ren, 80)) / 100.0 * 0.25 + min(ci / 500.0, 0.2)
    base = 0.45 if 8 <= hora <= 21 else 0.28
    return base + stress + random.uniform(-0.05, 0.08)


def lecturas_subestaciones(nodos: Dict, em: Dict, hora: int) -> Dict[str, Dict]:
    """Carga (MW) y voltaje (kV) por subestación, modulado por Electricity Maps."""
    factor_red = _factor_demanda_desde_em(em, hora)
    estados = {}
    for nid, datos in nodos.items():
        cap_mw = datos.get("capacidad_mw", 200)
        carga_mw = round(min(cap_mw * 1.02, cap_mw * factor_red * random.uniform(0.88, 1.12)), 2)
        uso_pct = carga_mw / cap_mw
        if uso_pct > 0.95:
            voltaje_kv = round(220 - (uso_pct - 0.9) * 100, 2)
            estado, motivo = "sobrecarga", "Carga >95% capacidad"
        elif uso_pct > 0.85:
            voltaje_kv = round(218 - random.uniform(0, 2), 2)
            estado, motivo = "alerta", "Carga elevada"
        else:
            voltaje_kv = round(220 - random.uniform(0, 1), 2)
            estado, motivo = "ok", "Operación normal"
        estados[nid] = {
            "voltaje_kv": voltaje_kv,
            "potencia_mw": carga_mw,
            "capacidad_mw": cap_mw,
            "uso_pct": round(uso_pct * 100, 1),
            "estado": estado,
            "motivo": motivo,
            "timestamp": datetime.now().isoformat(),
        }
    return estados


def estado_lineas(aristas: List, nodos: Dict) -> Dict[str, Dict]:
    estados = {}
    for t in aristas:
        src, dst = t[0], t[1]
        long_km = t[2] if len(t) > 2 else 100
        cap_mw = t[3] if len(t) > 3 else 300
        key = f"{src}|{dst}"
        flujo_mw = round(cap_mw * random.uniform(0.3, 0.95), 2)
        uso = flujo_mw / cap_mw
        if uso > 0.95:
            estado, motivo = "sobrecarga", "Flujo > límite térmico"
        elif uso > 0.85:
            estado, motivo = "alerta", "Flujo elevado"
        else:
            estado, motivo = "ok", "Operación normal"
        estados[key] = {
            "flujo_mw": flujo_mw,
            "capacidad_mw": cap_mw,
            "longitud_km": long_km,
            "estado": estado,
            "motivo": motivo,
            "timestamp": datetime.now().isoformat(),
        }
    return estados


def clima_zona_renovable(zona: Dict) -> Dict:
    """OpenWeather en coordenadas de planta solar/eólica."""
    try:
        r = requests.get(
            API_WEATHER_BASE,
            params={"lat": zona["lat"], "lon": zona["lon"], "appid": API_WEATHER_KEY, "units": "metric"},
            timeout=10,
        )
        r.raise_for_status()
        d = r.json()
        main = d.get("main", {})
        w = (d.get("weather") or [{}])[0]
        wind = d.get("wind", {})
        clouds = d.get("clouds", {})
        return {
            "zona_id": zona["zona_id"],
            "tipo_planta": zona["tipo"],
            "lat": zona["lat"],
            "lon": zona["lon"],
            "descripcion_zona": zona.get("descripcion", ""),
            "temperatura_c": main.get("temp"),
            "humedad_pct": main.get("humidity"),
            "presion_hpa": main.get("pressure"),
            "viento_ms": wind.get("speed"),
            "direccion_viento_gr": wind.get("deg"),
            "nubes_pct": clouds.get("all"),
            "descripcion_clima": w.get("description", ""),
            "timestamp": datetime.now().isoformat(),
        }
    except Exception as e:
        return {
            "zona_id": zona["zona_id"],
            "tipo_planta": zona["tipo"],
            "lat": zona["lat"],
            "lon": zona["lon"],
            "error": str(e),
            "timestamp": datetime.now().isoformat(),
        }


def payload_energy_raw(em: Dict, subestaciones: Dict, lineas: Dict) -> Dict:
    return {
        "timestamp": datetime.now().isoformat(),
        "intervalo_minutos": 15,
        "electricity_maps": em,
        "subestaciones": subestaciones,
        "lineas": lineas,
    }


def payload_weather_raw(zonas_mediciones: List[Dict]) -> Dict:
    return {
        "timestamp": datetime.now().isoformat(),
        "intervalo_minutos": 15,
        "zonas_renovables": zonas_mediciones,
    }


def _ensure_hdfs_path():
    try:
        import subprocess
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_BACKUP_PATH], capture_output=True, timeout=15)
    except Exception:
        pass


def guardar_en_hdfs(data: Dict) -> bool:
    try:
        import subprocess
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fn = f"{HDFS_BACKUP_PATH}/energy_{ts}.json"
        subprocess.run(
            ["hdfs", "dfs", "-put", "-", fn],
            input=json.dumps(data, ensure_ascii=False).encode("utf-8"),
            capture_output=True,
            timeout=15,
        )
        print(f"HDFS: {fn}")
        return True
    except Exception as e:
        print(f"HDFS error: {e}")
        return False


def publicar_kafka(data: Dict, topic: str) -> bool:
    try:
        from kafka import KafkaProducer
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
        p.send(topic, value=data).get(timeout=15)
        p.flush()
        p.close()
        print(f"Kafka: {topic}")
        return True
    except Exception as e:
        print(f"Kafka error ({topic}): {e}")
        return False


def ejecutar_ingesta() -> Tuple[Dict, Dict]:
    print("=" * 60)
    print("INGESTA - Electricity Maps + energy_raw + weather_raw")
    print("=" * 60)

    nodos = get_nodos()
    aristas = get_aristas()
    hora = datetime.now().hour

    print("[1] Electricity Maps...")
    em = obtener_electricity_maps()
    print(f"    Zona {em.get('zone')}: CI={em.get('carbon_intensity_g_co2_kwh')} gCO2/kWh, renovable~{em.get('renewable_pct')}%")

    print("[2] Lecturas carga/voltaje por subestación -> energy_raw")
    sub = lecturas_subestaciones(nodos, em, hora)
    lin = estado_lineas(aristas, nodos)
    energy = payload_energy_raw(em, sub, lin)

    print("[3] Clima zonas solares/eólicas -> weather_raw")
    zonas = []
    for z in ZONAS_RENOVABLES:
        zonas.append(clima_zona_renovable(z))
        time.sleep(0.15)
    weather = payload_weather_raw(zonas)

    _ensure_hdfs_path()
    guardar_en_hdfs(energy)
    publicar_kafka(energy, TOPIC_RAW)
    publicar_kafka(weather, TOPIC_WEATHER_RAW)

    print("INGESTA OK")

    # Opcional: persistir en Hive (sostenibilidad + clima renovables)
    if os.environ.get("PERSIST_HIVE_AFTER_INGEST", "").lower() in ("1", "true", "yes"):
        try:
            import tempfile
            d = Path(tempfile.gettempdir())
            fe, fw = d / "smart_grid_last_energy.json", d / "smart_grid_last_weather.json"
            fe.write_text(json.dumps(energy, ensure_ascii=False), encoding="utf-8")
            fw.write_text(json.dumps(weather, ensure_ascii=False), encoding="utf-8")
            script = Path(__file__).resolve().parent / "procesamiento" / "persistir_hive_ingesta.py"
            subprocess.run(
                [sys.executable, str(script), "--energy", str(fe), "--weather", str(fw)],
                cwd=str(Path(__file__).resolve().parent),
                timeout=120,
                capture_output=True,
                text=True,
            )
        except Exception as ex:
            print(f"[Hive post-ingesta] {ex}")

    return energy, weather


if __name__ == "__main__":
    ejecutar_ingesta()
