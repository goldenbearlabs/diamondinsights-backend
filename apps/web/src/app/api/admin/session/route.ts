import { NextRequest, NextResponse } from "next/server";

import {
  adminUnauthorizedResponse,
  authHeaderFromCookie,
  backendApiUrl,
  backendUnavailableResponse,
} from "@/lib/admin-api";

export async function GET(request: NextRequest) {
  const authorization = authHeaderFromCookie(request);
  if (!authorization) {
    return NextResponse.json({ authenticated: false });
  }

  let upstream: Response;
  try {
    upstream = await fetch(backendApiUrl("/admin/auth/check"), {
      method: "GET",
      headers: { Authorization: authorization },
      cache: "no-store",
    });
  } catch {
    return backendUnavailableResponse();
  }

  if (upstream.status === 401) {
    return adminUnauthorizedResponse("Unauthorized");
  }
  if (!upstream.ok) {
    return NextResponse.json({ authenticated: false }, { status: 502 });
  }

  return NextResponse.json({ authenticated: true });
}
