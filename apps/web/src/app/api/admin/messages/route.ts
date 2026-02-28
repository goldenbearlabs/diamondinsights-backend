import { NextRequest, NextResponse } from "next/server";

import { backendApiUrl, backendUnavailableResponse } from "@/lib/backend-api";
import {
  adminUnauthorizedResponse,
  authHeaderFromCookie,
} from "@/lib/admin-api";

export async function GET(request: NextRequest) {
  const authorization = authHeaderFromCookie(request);
  if (!authorization) {
    return adminUnauthorizedResponse();
  }

  const limit = request.nextUrl.searchParams.get("limit") || "100";
  let upstream: Response;
  try {
    upstream = await fetch(backendApiUrl(`/admin/messages?limit=${encodeURIComponent(limit)}`), {
      method: "GET",
      headers: { Authorization: authorization },
      cache: "no-store",
    });
  } catch {
    return backendUnavailableResponse();
  }

  if (upstream.status === 401) {
    return adminUnauthorizedResponse();
  }

  const payload = await upstream.json().catch(() => null);
  if (!upstream.ok) {
    return NextResponse.json(payload || { detail: "Failed to load messages" }, { status: upstream.status });
  }

  return NextResponse.json(payload);
}
