import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";

function upstreamBase(): string {
  return (process.env.BACKEND_INTERNAL_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
}

function targetUrl(req: NextRequest, segments: string[] | undefined): string {
  const path = segments?.length ? `/${segments.join("/")}` : "";
  return `${upstreamBase()}${path}${req.nextUrl.search}`;
}

const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailers",
  "transfer-encoding",
  "upgrade",
]);

async function proxy(req: NextRequest, ctx: { params: { path?: string[] } }) {
  const url = targetUrl(req, ctx.params.path);
  const headers = new Headers();
  req.headers.forEach((value, key) => {
    const k = key.toLowerCase();
    if (k === "host" || HOP_BY_HOP.has(k)) return;
    headers.set(key, value);
  });

  let body: ArrayBuffer | undefined;
  if (!["GET", "HEAD"].includes(req.method)) {
    body = await req.arrayBuffer();
  }

  const init: RequestInit = {
    method: req.method,
    headers,
    redirect: "manual",
  };
  if (body !== undefined && body.byteLength > 0) {
    init.body = body;
  }

  let upstream: Response;
  try {
    upstream = await fetch(url, init);
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    return NextResponse.json(
      { detail: `Proxy error hacia ${upstreamBase()}: ${msg}` },
      { status: 502 },
    );
  }

  const out = new Headers();
  upstream.headers.forEach((value, key) => {
    const k = key.toLowerCase();
    if (HOP_BY_HOP.has(k)) return;
    out.set(key, value);
  });

  const buf = await upstream.arrayBuffer();
  return new NextResponse(buf, {
    status: upstream.status,
    statusText: upstream.statusText,
    headers: out,
  });
}

export async function GET(req: NextRequest, ctx: { params: { path?: string[] } }) {
  return proxy(req, ctx);
}

export async function POST(req: NextRequest, ctx: { params: { path?: string[] } }) {
  return proxy(req, ctx);
}

export async function PUT(req: NextRequest, ctx: { params: { path?: string[] } }) {
  return proxy(req, ctx);
}

export async function PATCH(req: NextRequest, ctx: { params: { path?: string[] } }) {
  return proxy(req, ctx);
}

export async function DELETE(req: NextRequest, ctx: { params: { path?: string[] } }) {
  return proxy(req, ctx);
}

export async function OPTIONS() {
  return new NextResponse(null, { status: 204 });
}
