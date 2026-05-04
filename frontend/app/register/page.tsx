"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { apiFetch } from "@/lib/api";

export default function RegisterPage() {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [tenantName, setTenantName] = useState("");
  const [err, setErr] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault();
    setErr(null);
    setLoading(true);
    try {
      await apiFetch("/auth/register", {
        method: "POST",
        body: JSON.stringify({
          email,
          password,
          tenant_name: tenantName.trim() || null,
        }),
      });
      router.push("/login");
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : "Error");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="card" style={{ maxWidth: 520 }}>
      <h2 style={{ marginTop: 0 }}>Crear cuenta</h2>
      <p className="muted" style={{ marginTop: 0 }}>
        El primer usuario del sistema recibe rol <strong>admin</strong> automáticamente.
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
          Contraseña (mín. 8)
          <input
            type="password"
            name="password"
            autoComplete="new-password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            minLength={8}
            required
            style={{ width: "100%", marginTop: 6 }}
          />
        </label>
        <label className="muted">
          Organización / tenant (opcional)
          <input
            name="organization"
            autoComplete="organization"
            value={tenantName}
            onChange={(e) => setTenantName(e.target.value)}
            style={{ width: "100%", marginTop: 6 }}
          />
        </label>
        {err && <p style={{ color: "#f87171" }}>{err}</p>}
        <button type="submit" disabled={loading}>
          {loading ? "Creando…" : "Registrarse"}
        </button>
      </form>
    </div>
  );
}
