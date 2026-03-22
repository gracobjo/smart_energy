#!/usr/bin/bash
# Arranca la API REST Smart Grid (Swagger en /docs).
# Uso: ./scripts/iniciar_api_smart_grid.sh
# Variables: API_SMART_GRID_PORT (default 8000)
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE"

PORT="${API_SMART_GRID_PORT:-8000}"
if command -v nc &>/dev/null && nc -z 127.0.0.1 "$PORT" 2>/dev/null; then
  echo "API Smart Grid ya responde en puerto $PORT. Swagger: http://localhost:$PORT/docs"
  exit 0
fi

echo "Arrancando API Smart Grid en puerto $PORT..."
echo "  Swagger UI: http://localhost:$PORT/docs"
echo "  ReDoc:      http://localhost:$PORT/redoc"
echo "  OpenAPI:    http://localhost:$PORT/openapi.json"
echo ""

export API_SMART_GRID_PORT="$PORT"
# Usar venv si existe
PYTHON="${BASE}/venv/bin/python"
[[ -x "$PYTHON" ]] || PYTHON="${BASE}/venv_transporte/bin/python"
[[ -x "$PYTHON" ]] || PYTHON="python3"
exec "$PYTHON" -m uvicorn api.main:app --host 0.0.0.0 --port "$PORT"
