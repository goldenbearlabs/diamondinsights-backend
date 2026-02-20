import { NextRequest, NextResponse } from "next/server";

import {
  adminUnauthorizedResponse,
  authHeaderFromCookie,
  backendApiUrl,
  backendUnavailableResponse,
} from "@/lib/admin-api";

type RosterSettingsBody = {
  next_roster_update_at: string | null;
};

export async function GET(request: NextRequest) {
  const authorization = authHeaderFromCookie(request);
  if (!authorization) {
    return adminUnauthorizedResponse();
  }

  let upstream: Response;
  try {
    upstream = await fetch(backendApiUrl("/admin/roster-settings"), {
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
    return NextResponse.json(payload || { detail: "Failed to load roster settings" }, { status: upstream.status });
  }

  return NextResponse.json(payload);
}

export async function PUT(request: NextRequest) {
  const authorization = authHeaderFromCookie(request);
  if (!authorization) {
    return adminUnauthorizedResponse();
  }

  let body: RosterSettingsBody;
  try {
    body = (await request.json()) as RosterSettingsBody;
  } catch {
    return NextResponse.json({ detail: "Invalid request body" }, { status: 400 });
  }

  let upstream: Response;
  try {
    upstream = await fetch(backendApiUrl("/admin/roster-settings"), {
      method: "PUT",
      headers: {
        Authorization: authorization,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        next_roster_update_at: body.next_roster_update_at,
      }),
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
    return NextResponse.json(payload || { detail: "Failed to update roster settings" }, { status: upstream.status });
  }

  return NextResponse.json(payload);
}
