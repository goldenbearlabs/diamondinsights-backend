import { NextResponse } from "next/server";

const DEFAULT_BACKEND_URL = "http://localhost:8000";

function backendBaseUrl(): string {
  const fromBackendEnv = (process.env.BACKEND_API_URL || "").trim();
  const fromPublicEnv = (process.env.NEXT_PUBLIC_API_BASE_URL || "").trim();
  return (fromBackendEnv || fromPublicEnv || DEFAULT_BACKEND_URL).replace(/\/+$/, "");
}

export function backendApiUrl(path: string): string {
  const suffix = path.startsWith("/") ? path : `/${path}`;
  return `${backendBaseUrl()}${suffix}`;
}

export function backendUnavailableResponse(): NextResponse {
  return NextResponse.json(
    { detail: "Backend API unavailable. Verify BACKEND_API_URL and backend uptime." },
    { status: 502 },
  );
}
