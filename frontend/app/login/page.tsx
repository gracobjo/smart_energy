"use client";

import Link from "next/link";
import { useState } from "react";
import { useRouter } from "next/navigation";
import { apiFetch, setToken } from "@/lib/api";

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setErr(null);
    setLoading(true);
    try {
      const res = await apiFetch<{ access_token: string }>("/auth/login", {
        method: "POST",
        body: JSON.stringify({ email, password }),
      });
      setToken(res.access_token);
      router.push("/dashboard");
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "Error");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="card" style={{ maxWidth: 480 }}>
      <h2 style={{ marginTop: 0 }}>Iniciar sesión</h2>
      <p className="muted" style={{ marginTop: 0, fontSize: 14 }}>
        Si es la primera vez, ve a <Link href="/register">Registrarse</Link>. El primer usuario del sistema será{" "}
        <strong>admin</strong>. Usa un email válido (p. ej. <code>tu@correo.com</code>).
      </p>
      <form onSubmit={onSubmit} className="row" style={{ flexDirection: "column", alignItems: "stretch" }}>
        <label className="muted">
          Email
          <input
            type="email"
            name="email"
            autoComplete="email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
            style={{ width: "100%", marginTop: 6 }}
          />
        </label>
        <label className="muted">
          Contraseña
          <input
            type="password"
            name="password"
            autoComplete="current-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
            style={{ width: "100%", marginTop: 6 }}
          />
        </label>
        {err && <p style={{ color: "#f87171" }}>{err}</p>}
        <button type="submit" disabled={loading}>
          {loading ? "Entrando…" : "Entrar"}
        </button>
      </form>
    </div>
  );
}
