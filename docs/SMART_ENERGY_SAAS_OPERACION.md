# Smart Energy SaaS — Operación y resolución de problemas

Guía breve para quien despliega o desarrolla el MVP descrito en el [README principal](../README.md) (sección *Smart Energy SaaS*).

## Arranque y parada (Docker Compose)

Desde la raíz del repositorio:

```bash
# Levantar (reconstruye imágenes si cambió código o Dockerfile)
docker compose up --build

# Parar y eliminar contenedores (la red; los datos de Postgres siguen en el volumen)
docker compose down

# Parar y borrar también los datos de la base
docker compose down -v
```

Servicios por defecto:

| Servicio   | Puerto host | Uso |
|------------|-------------|-----|
| `frontend` | 3000        | Panel Next.js |
| `backend`  | 8000        | FastAPI y documentación `/docs` |
| `postgres` | 5432        | PostgreSQL (usuario/clave `smartenergy` / `smartenergy`) |

## Registro e inicio de sesión

- No hay usuario precreado: hay que **registrarse** en `/register`.
- El **primer usuario** registrado en esa base de datos recibe rol **admin**.
- El email debe ser válido para Pydantic (p. ej. `nombre@dominio.com`); `admin@admin` no es válido.
- Contraseña mínimo **8** caracteres.

## Cómo llega el frontend al API (Docker)

El navegador solo habla con **http://localhost:3000**. Las llamadas REST van a **`/ingest-api/...`** (mismo origen).

Un **Route Handler** de Next (`frontend/app/ingest-api/[[...path]]/route.ts`) reenvía la petición al backend usando la variable de entorno **`BACKEND_INTERNAL_URL`** (en Compose: `http://backend:8000`). Así se evitan errores de tipo **«Failed to fetch»** al llamar directamente a otro puerto u origen en Windows.

El **WebSocket** de energía sigue conectando desde el navegador a **`ws://127.0.0.1:8000`** (puerto publicado del backend en el host).

Variables relevantes en el build del frontend: `NEXT_PUBLIC_API_URL=/ingest-api`, `NEXT_PUBLIC_WS_URL` (ver `docker-compose.yml` y `frontend/Dockerfile`).

## Errores frecuentes

### 500 en `POST /ingest-api/auth/register`

- Revisar logs del contenedor **`backend`** (p. ej. fallo de base de datos o de hash de contraseña).
- El backend fija **`bcrypt<4.1`** por compatibilidad con **passlib**; si se actualizan dependencias, comprobar de nuevo el registro.

### «Failed to fetch» hacia `localhost:8000` desde el front

- En Docker debe usarse el flujo **`/ingest-api`** descrito arriba; no hace falta exponer el API al navegador en otro origen para el panel.

### Errores en consola del tipo «message channel closed»

- Suelen venir de **extensiones del navegador**, no del código de la aplicación. Probar en ventana de incógnito sin extensiones.

### Migraciones Alembic vs tablas ya existentes

Si la base se creó antes solo con `create_all` y Alembic falla por tablas duplicadas, desde `backend/`:

```bash
alembic stamp 0001_initial
```

Detalle en el README (sección *Migraciones*).

## Archivos clave

| Ruta | Descripción |
|------|-------------|
| `docker-compose.yml` | Orquestación Postgres + backend + frontend |
| `.env.example` | Variables de ejemplo (JWT, notificaciones, Alembic) |
| `backend/alembic/` | Migraciones SQL |
| `frontend/app/ingest-api/[[...path]]/route.ts` | Proxy HTTP al backend |
