# Credenciales UI — Smart Grid

Credenciales para acceder a las interfaces web del sistema.

---

## Airflow

| Campo | Valor |
|-------|-------|
| **URL** | http://localhost:8080 (o http://&lt;IP_SERVIDOR&gt;:8080) |
| **Usuario** | `admin` |
| **Contraseña** | Ver sección *Obtener contraseña* según tu versión |

### Obtener contraseña (Airflow 3.x)

En **Airflow 3.x** el sistema de autenticación por defecto es **SimpleAuthManager**. El comando `airflow users create` no existe; las contraseñas se gestionan de forma distinta.

#### Opción A: SimpleAuthManager (por defecto) — contraseña auto-generada

La contraseña del usuario `admin` se genera automáticamente y está en:

```bash
cat ~/airflow/simple_auth_manager_passwords.json.generated
```

O en el log del api-server al primer arranque:

```bash
grep -i "password\|admin" /tmp/smart_grid_airflow_api.log
```

#### Opción B: Sin autenticación (solo desarrollo)

Edita `~/airflow/airflow.cfg` y en `[core]` añade:

```ini
simple_auth_manager_all_admins = True
```

Reinicia Airflow. Permite acceso sin contraseña (todos como admin).

#### Opción C: FAB (admin/admin clásico)

Para usar `admin` / `admin` con el comando `airflow users create`:

1. Instala el provider FAB:
   ```bash
   pip install apache-airflow-providers-fab
   ```

2. Edita `~/airflow/airflow.cfg` en `[core]`:
   ```ini
   auth_manager = airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
   ```

3. Reinicia Airflow y crea el usuario:
   ```bash
   airflow users create --role Admin --username admin --email admin@localhost \
     --firstname Admin --lastname User --password admin
   ```

### Airflow 2.x (si aplica)

Con Airflow 2.x (FAB por defecto), el usuario se crea con:

```bash
airflow users create --role Admin --username admin --email admin@localhost \
  --firstname Admin --lastname User --password admin
```

Con Docker: el usuario `admin` / `admin` se crea automáticamente.

---

## NiFi

| Campo | Valor |
|-------|-------|
| **URL** | https://localhost:8443/nifi (o https://&lt;IP_SERVIDOR&gt;:8443/nifi) |
| **Usuario** | `nifi` (o ver `login-identity-providers.xml` / `nifi-app.log`) |
| **Contraseña** | La que definiste con `set-single-user-credentials`, o en log si se auto-generó |

### Obtener / definir credenciales

**Si se auto-generaron** (Username/Password vacíos al instalar):
```bash
grep -E "Generated (Username|Password)" $NIFI_HOME/logs/nifi-app.log
```

**Si usaste credenciales manuales** (o no aparecen en el log), define una contraseña conocida:
```bash
$NIFI_HOME/bin/nifi.sh stop
$NIFI_HOME/bin/nifi.sh set-single-user-credentials nifi TU_CONTRASEÑA
$NIFI_HOME/bin/nifi.sh start
```
Luego en `.env`: `NIFI_USER=nifi` y `NIFI_PASS=TU_CONTRASEÑA`.

### El enlace a NiFi no abre (No se puede conectar)

NiFi **escucha en 127.0.0.1 por defecto**. Si el dashboard genera un enlace con la IP de la interfaz (ej. 10.0.2.15 en VM), el navegador puede no conectar.

**Opciones:**

1. **Usa localhost** desde la misma máquina: `https://localhost:8443/nifi`
2. **Haz que NiFi escuche en todas las interfaces:**
   ```bash
   ./scripts/patch_nifi_bind_all_interfaces.sh
   $NIFI_HOME/bin/nifi.sh stop
   $NIFI_HOME/bin/nifi.sh start
   ```
   Tras reiniciar, `https://<IP_SERVIDOR>:8443/nifi` será accesible.

---

## Sincronizar DAGs de Airflow

Los DAGs del proyecto están en `orquestacion/`. Para que Airflow los cargue hay que copiarlos a `~/airflow/dags/`.

### Script de sincronización (recomendado)

```bash
cd ~/smart_energy
export AIRFLOW_HOME=~/airflow
./scripts/sync_dags_airflow.sh
```

Esto copia todos los `dag_*.py` de `orquestacion/` a `~/airflow/dags/`.

### Verificar que se copiaron

```bash
ls -la ~/airflow/dags/dag_*.py
```

Deben aparecer los 10 DAGs del proyecto.

### Variable SMART_ENERGY_HOME

Los DAGs usan `SMART_ENERGY_HOME` para encontrar scripts del proyecto. Si se ejecutan desde `~/airflow/dags` (copia), define:

```bash
export SMART_ENERGY_HOME=/home/hadoop/smart_energy
```

Añádelo a `~/.bashrc` o al entorno donde arrancas Airflow (p. ej. `scripts/iniciar_servicios.sh` ya exporta variables del proyecto).

### Cuándo sincronizar

- Tras clonar o actualizar el repositorio.
- Tras modificar DAGs en `orquestacion/`.
- Si la UI de Airflow muestra "0 DAGs" o faltan DAGs: sincroniza y reinicia el **dag-processor** (ver `docs/AIRFLOW.md`).

---

## Acceso desde el frontend

El dashboard (`app_visualizacion.py`) muestra enlaces a Airflow y NiFi en:

- **Barra lateral** → sección "UIs (Airflow, NiFi)"
- **Monitorización** (expander) → botones Airflow y NiFi

Los enlaces usan el host del servidor (IP detectada o `HADOOP_UI_HOST`).
