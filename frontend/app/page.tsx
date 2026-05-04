import Link from "next/link";

export default function HomePage() {
  return (
    <div className="card">
      <h1 style={{ marginTop: 0 }}>Smart Energy Monitoring</h1>
      <p className="muted">
        MVP SaaS: FastAPI + PostgreSQL + WebSockets + modelos scikit-learn en el módulo <code>ai/</code>.
      </p>
      <div className="row" style={{ marginTop: "1rem" }}>
        <Link href="/login">
          <button type="button">Iniciar sesión</button>
        </Link>
        <Link href="/register">
          <button type="button" className="secondary">
            Registrarse
          </button>
        </Link>
        <Link href="/dashboard">
          <button type="button" className="secondary">
            Ir al panel
          </button>
        </Link>
      </div>
    </div>
  );
}
