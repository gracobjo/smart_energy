"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { clearToken, getToken } from "@/lib/api";

const links = [
  { href: "/dashboard", label: "Panel" },
  { href: "/devices", label: "Dispositivos" },
  { href: "/alerts", label: "Alertas" },
];

export function Nav() {
  const pathname = usePathname();
  const router = useRouter();
  const [authed, setAuthed] = useState(false);
  useEffect(() => {
    setAuthed(!!getToken());
  }, [pathname]);

  return (
    <header
      style={{
        borderBottom: "1px solid #1f2a44",
        background: "#0f172a",
      }}
    >
      <div
        style={{
          maxWidth: 1100,
          margin: "0 auto",
          padding: "0.75rem 1.25rem",
          display: "flex",
          gap: "1rem",
          alignItems: "center",
          flexWrap: "wrap",
        }}
      >
        <Link href="/" style={{ fontWeight: 800, color: "#e8eefc" }}>
          Smart Energy
        </Link>
        {links.map((l) => (
          <Link
            key={l.href}
            href={l.href}
            style={{
              color: pathname === l.href ? "#38bdf8" : "#cbd5e1",
              fontWeight: pathname === l.href ? 700 : 500,
            }}
          >
            {l.label}
          </Link>
        ))}
        <span style={{ flex: 1 }} />
        {!authed ? (
          <>
            <Link href="/login">Entrar</Link>
            <Link href="/register">Registro</Link>
          </>
        ) : (
          <button
            type="button"
            className="secondary"
            onClick={() => {
              clearToken();
              router.push("/login");
            }}
          >
            Salir
          </button>
        )}
      </div>
    </header>
  );
}
