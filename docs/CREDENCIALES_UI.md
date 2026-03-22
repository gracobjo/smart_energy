# Credenciales UI — Smart Grid

Credenciales para acceder a las interfaces web del sistema.

---

## Airflow

| Campo | Valor |
|-------|-------|
| **URL** | http://localhost:8080 (o http://&lt;IP_SERVIDOR&gt;:8080) |
| **Usuario** | `admin` |
| **Contraseña** | `admin` |

### Primera configuración

Si es la primera vez y el usuario no existe:

```bash
airflow users create --role Admin --username admin --email admin@localhost \
  --firstname Admin --lastname User --password admin
```

Con Docker (docker-compose.airflow.yml): el usuario `admin` / `admin` se crea automáticamente.

---

## NiFi

| Campo | Valor |
|-------|-------|
| **URL** | https://localhost:8443/nifi (o https://&lt;IP_SERVIDOR&gt;:8443/nifi) |
| **Usuario** | Ver `nifi-app.log` ("Generated Username") |
| **Contraseña** | Ver `nifi-app.log` ("Generated Password") |

### Obtener credenciales

```bash
grep -E "Generated (Username|Password)" $NIFI_HOME/logs/nifi-app.log
```

Tras la instalación con `scripts/instalar_nifi_260.sh`, las credenciales se generan en el primer arranque.

---

## Acceso desde el frontend

El dashboard (`app_visualizacion.py`) muestra enlaces a Airflow y NiFi en:

- **Barra lateral** → sección "UIs (Airflow, NiFi)"
- **Monitorización** (expander) → botones Airflow y NiFi

Los enlaces usan el host del servidor (IP detectada o `HADOOP_UI_HOST`).

---

## Sincronizar DAGs

Si los DAGs están en `~/airflow/dags` (copia), define `SMART_ENERGY_HOME` para que apunten al proyecto:

```bash
export SMART_ENERGY_HOME=/home/hadoop/smart_energy
```

O usa el script de sincronización:

```bash
./scripts/sync_dags_airflow.sh
```
