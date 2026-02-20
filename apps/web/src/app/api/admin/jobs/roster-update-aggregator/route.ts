import { NextRequest, NextResponse } from "next/server";

import {
  adminUnauthorizedResponse,
  authHeaderFromCookie,
  backendApiUrl,
  backendUnavailableResponse,
} from "@/lib/admin-api";

type EnqueueBody = {
  confirm_text?: string;
};

export async function POST(request: NextRequest) {
  const authorization = authHeaderFromCookie(request);
  if (!authorization) {
    return adminUnauthorizedResponse();
  }

  let body: EnqueueBody;
  try {
    body = (await request.json()) as EnqueueBody;
  } catch {
    return NextResponse.json({ detail: "Invalid request body" }, { status: 400 });
  }

  let upstream: Response;
  try {
    upstream = await fetch(backendApiUrl("/admin/jobs/roster-update-aggregator"), {
      method: "POST",
      headers: {
        Authorization: authorization,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ confirm_text: body.confirm_text || "" }),
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
    return NextResponse.json(payload || { detail: "Failed to enqueue roster-update-aggregator" }, { status: upstream.status });
  }

  return NextResponse.json(payload);
}
