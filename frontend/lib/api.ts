/**
 * URL absoluta del API (p. ej. http://127.0.0.1:8000) o prefijo path con proxy
 * (p. ej. /ingest-api) cuando BACKEND_INTERNAL_URL está definido en el build.
 */
const rawApi = (process.env.NEXT_PUBLIC_API_URL ?? "http://127.0.0.1:8000").replace(/\/$/, "");
export const API_BASE = rawApi;

export const WS_BASE = (process.env.NEXT_PUBLIC_WS_URL ?? "ws://127.0.0.1:8000").replace(/\/$/, "");

/** Construye la URL de petición (absoluta o relativa al origen de la página). */
export function apiUrl(path: string): string {
  const p = path.startsWith("/") ? path : `/${path}`;
  if (API_BASE.startsWith("http")) return `${API_BASE}${p}`;
  return `${API_BASE}${p}`;
}

export const TOKEN_KEY = "smartenergy_token";

export function getToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string) {
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
}

/** Convierte `detail` de FastAPI/Pydantic (string | lista) en texto legible. */
export function formatApiErrorDetail(detail: unknown): string {
  if (detail == null) return "Error desconocido";
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const parts = detail.map((item: { msg?: string; loc?: unknown[] }) => {
      const loc = Array.isArray(item.loc)
        ? item.loc.filter((x) => x !== "body" && typeof x === "string").join(" → ")
        : "";
      const msg = item.msg ?? JSON.stringify(item);
      return loc ? `${loc}: ${msg}` : msg;
    });
    return parts.join(" · ") || JSON.stringify(detail);
  }
  if (typeof detail === "object" && detail !== null && "msg" in detail) {
    return String((detail as { msg: string }).msg);
  }
  return JSON.stringify(detail);
}

export async function apiFetch<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set("Content-Type", "application/json");
  const t = getToken();
  if (t) headers.set("Authorization", `Bearer ${t}`);
  let res: Response;
  try {
    res = await fetch(apiUrl(path), { ...init, headers });
  } catch (e) {
    const docsHint = API_BASE.startsWith("http") ? `${API_BASE}/docs` : "http://127.0.0.1:8000/docs";
    const hint =
      `No se pudo conectar con la API (${apiUrl(path)}). ` +
      `Comprueba el contenedor «backend» y abre ${docsHint}. ` +
      `Si acabas de levantar Docker, espera a que el backend esté «healthy».`;
    if (e instanceof TypeError) throw new Error(hint);
    throw e;
  }
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const j = await res.json();
      if (j?.detail != null) detail = formatApiErrorDetail(j.detail);
    } catch {
      /* ignore */
    }
    throw new Error(detail);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}
