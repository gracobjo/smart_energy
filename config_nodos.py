"""
Sistema de Monitoreo de Redes de Energía Inteligentes (Smart Grid) - España
Configuración de la red eléctrica: subestaciones (nodos) y líneas de alta tensión (aristas).
Mapeo desde dominio transporte: Almacén/Parada → Subestación, Ruta/Carretera → Línea de Alta Tensión.
"""

# =============================================================================
# 5 SUBESTACIONES PRINCIPALES (hubs de la red)
# =============================================================================
SUBESTACIONES_PRINCIPALES = {
    "Madrid": {"lat": 40.4168, "lon": -3.7038, "tipo": "principal", "capacidad_mw": 800},
    "Barcelona": {"lat": 41.3851, "lon": 2.1734, "tipo": "principal", "capacidad_mw": 700},
    "Bilbao": {"lat": 43.2630, "lon": -2.9350, "tipo": "principal", "capacidad_mw": 500},
    "Vigo": {"lat": 42.2406, "lon": -8.7207, "tipo": "principal", "capacidad_mw": 400},
    "Sevilla": {"lat": 37.3891, "lon": -5.9845, "tipo": "principal", "capacidad_mw": 600},
}

# =============================================================================
# 25 SUBESTACIONES SECUNDARIAS (distribución por zona)
# =============================================================================
SUBESTACIONES_SECUNDARIAS = {
    "Toledo": {"lat": 39.8628, "lon": -4.0273, "hub": "Madrid", "capacidad_mw": 150},
    "Segovia": {"lat": 40.9429, "lon": -4.1088, "hub": "Madrid", "capacidad_mw": 120},
    "Guadalajara": {"lat": 40.6289, "lon": -3.1614, "hub": "Madrid", "capacidad_mw": 130},
    "Cuenca": {"lat": 40.0718, "lon": -2.1340, "hub": "Madrid", "capacidad_mw": 100},
    "Ávila": {"lat": 40.6564, "lon": -4.6814, "hub": "Madrid", "capacidad_mw": 110},
    "Tarragona": {"lat": 41.1189, "lon": 1.2445, "hub": "Barcelona", "capacidad_mw": 180},
    "Girona": {"lat": 41.9794, "lon": 2.8214, "hub": "Barcelona", "capacidad_mw": 140},
    "Lleida": {"lat": 41.6176, "lon": 0.6200, "hub": "Barcelona", "capacidad_mw": 120},
    "Manresa": {"lat": 41.7250, "lon": 1.8260, "hub": "Barcelona", "capacidad_mw": 100},
    "Sabadell": {"lat": 41.5499, "lon": 2.1103, "hub": "Barcelona", "capacidad_mw": 160},
    "Santander": {"lat": 43.4647, "lon": -3.8044, "hub": "Bilbao", "capacidad_mw": 130},
    "Vitoria": {"lat": 42.8467, "lon": -2.6727, "hub": "Bilbao", "capacidad_mw": 140},
    "San Sebastián": {"lat": 43.3183, "lon": -1.9812, "hub": "Bilbao", "capacidad_mw": 120},
    "Pamplona": {"lat": 42.8125, "lon": -1.6458, "hub": "Bilbao", "capacidad_mw": 110},
    "Logroño": {"lat": 42.4627, "lon": -2.4450, "hub": "Bilbao", "capacidad_mw": 100},
    "A Coruña": {"lat": 43.3614, "lon": -8.4112, "hub": "Vigo", "capacidad_mw": 150},
    "Santiago": {"lat": 42.8805, "lon": -8.5457, "hub": "Vigo", "capacidad_mw": 120},
    "Pontevedra": {"lat": 42.4310, "lon": -8.6444, "hub": "Vigo", "capacidad_mw": 100},
    "Ourense": {"lat": 42.3358, "lon": -7.8639, "hub": "Vigo", "capacidad_mw": 110},
    "Lugo": {"lat": 43.0097, "lon": -7.5560, "hub": "Vigo", "capacidad_mw": 90},
    "Córdoba": {"lat": 37.8882, "lon": -4.7794, "hub": "Sevilla", "capacidad_mw": 180},
    "Málaga": {"lat": 36.7213, "lon": -4.4214, "hub": "Sevilla", "capacidad_mw": 200},
    "Cádiz": {"lat": 36.5271, "lon": -6.2886, "hub": "Sevilla", "capacidad_mw": 140},
    "Huelva": {"lat": 37.2614, "lon": -6.9447, "hub": "Sevilla", "capacidad_mw": 130},
    "Jerez": {"lat": 36.6850, "lon": -6.1273, "hub": "Sevilla", "capacidad_mw": 120},
}

# Compatibilidad con código que use HUBS/SECUNDARIOS
HUBS = SUBESTACIONES_PRINCIPALES
SECUNDARIOS = SUBESTACIONES_SECUNDARIAS


def get_nodos() -> dict:
    """
    Retorna diccionario de subestaciones: id -> {lat, lon, tipo, hub?, capacidad_mw}
    """
    nodos = {}
    for nombre, datos in SUBESTACIONES_PRINCIPALES.items():
        nodos[nombre] = {
            "lat": datos["lat"],
            "lon": datos["lon"],
            "tipo": "principal",
            "capacidad_mw": datos.get("capacidad_mw", 500),
        }
    for nombre, datos in SUBESTACIONES_SECUNDARIAS.items():
        nodos[nombre] = {
            "lat": datos["lat"],
            "lon": datos["lon"],
            "tipo": "secundario",
            "hub": datos["hub"],
            "capacidad_mw": datos.get("capacidad_mw", 100),
        }
    return nodos


# Malla principal: líneas de alta tensión entre subestaciones principales
HUB_NAMES = list(SUBESTACIONES_PRINCIPALES.keys())
MALLA_PRINCIPAL = [
    ("Madrid", "Barcelona"),
    ("Madrid", "Bilbao"),
    ("Madrid", "Vigo"),
    ("Madrid", "Sevilla"),
    ("Barcelona", "Bilbao"),
    ("Barcelona", "Vigo"),
    ("Barcelona", "Sevilla"),
    ("Bilbao", "Vigo"),
    ("Bilbao", "Sevilla"),
    ("Vigo", "Sevilla"),
]

# Conexiones estrella: secundarias a su subestación principal
CONEXIONES_ESTRELLA = [(nombre, datos["hub"]) for nombre, datos in SUBESTACIONES_SECUNDARIAS.items()]

# Conexiones entre subestaciones secundarias (redundancia de red)
CONEXIONES_SECUNDARIOS = [
    ("Toledo", "Cuenca"),
    ("Segovia", "Ávila"),
    ("Guadalajara", "Cuenca"),
    ("Tarragona", "Lleida"),
    ("Girona", "Sabadell"),
    ("Manresa", "Lleida"),
    ("Santander", "San Sebastián"),
    ("Vitoria", "Pamplona"),
    ("San Sebastián", "Pamplona"),
    ("Logroño", "Vitoria"),
    ("A Coruña", "Santiago"),
    ("Santiago", "Pontevedra"),
    ("Pontevedra", "Ourense"),
    ("Ourense", "Lugo"),
    ("Córdoba", "Málaga"),
    ("Málaga", "Cádiz"),
    ("Huelva", "Jerez"),
    ("Jerez", "Cádiz"),
]


def get_aristas() -> list:
    """
    Retorna lista de líneas de alta tensión: (src, dst, longitud_km, capacidad_mw).
    longitud_km se calcula por Haversine; capacidad_mw típica por línea.
    """
    import math
    nodos = get_nodos()

    def haversine_km(lat1, lon1, lat2, lon2):
        R = 6371
        phi1, phi2 = math.radians(lat1), math.radians(lat2)
        dphi = math.radians(lat2 - lat1)
        dlambda = math.radians(lon2 - lon1)
        a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
        return round(2 * R * math.asin(math.sqrt(a)), 2)

    aristas = []
    processed = set()

    def add_edge(a, b, capacidad_mw=300):
        key = tuple(sorted([a, b]))
        if key not in processed and a in nodos and b in nodos:
            dist = haversine_km(nodos[a]["lat"], nodos[a]["lon"], nodos[b]["lat"], nodos[b]["lon"])
            aristas.append((a, b, dist, capacidad_mw))
            processed.add(key)

    for src, dst in MALLA_PRINCIPAL:
        add_edge(src, dst, 400)
    for src, dst in CONEXIONES_ESTRELLA:
        add_edge(src, dst, 200)
    for src, dst in CONEXIONES_SECUNDARIOS:
        add_edge(src, dst, 150)

    return aristas


RED = {
    "nodos": get_nodos(),
    "aristas": get_aristas(),
    "hubs": list(SUBESTACIONES_PRINCIPALES.keys()),
    "secundarios": list(SUBESTACIONES_SECUNDARIAS.keys()),
}
