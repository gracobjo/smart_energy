"""
Zonas con plantas solares y eólicas (referencia geográfica para weather_raw).
El producer publica condiciones climáticas por zona en Kafka weather_raw.
"""
ZONAS_RENOVABLES = [
    {"zona_id": "solar_sevilla", "tipo": "solar", "lat": 37.3891, "lon": -5.9845, "descripcion": "Andalucía solar"},
    {"zona_id": "solar_extremadura", "tipo": "solar", "lat": 38.915, "lon": -6.343, "descripcion": "Extremadura fotovoltaica"},
    {"zona_id": "eolico_galicia", "tipo": "eolico", "lat": 43.3614, "lon": -8.4112, "descripcion": "Galicia eólica"},
    {"zona_id": "eolico_aragon", "tipo": "eolico", "lat": 41.6561, "lon": -0.8773, "descripcion": "Aragón eólico"},
    {"zona_id": "solar_murcia", "tipo": "solar", "lat": 37.9922, "lon": -1.1307, "descripcion": "Región Murcia solar"},
    {"zona_id": "eolico_castilla", "tipo": "eolico", "lat": 41.6523, "lon": -4.7245, "descripcion": "Castilla y León eólica"},
]
