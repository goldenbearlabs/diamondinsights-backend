import { NextRequest, NextResponse } from "next/server";

import {
  adminUnauthorizedResponse,
  authHeaderFromCookie,
  backendApiUrl,
  backendUnavailableResponse,
} from "@/lib/admin-api";

type Params = {
  params: Promise<{ id: string }>;
};

export async function DELETE(request: NextRequest, { params }: Params) {
  const authorization = authHeaderFromCookie(request);
  if (!authorization) {
    return adminUnauthorizedResponse();
  }

  const { id } = await params;
  if (!id || id === "undefined" || id === "null") {
    return NextResponse.json({ detail: "Invalid message id" }, { status: 400 });
  }

  const numericId = Number(id);
  if (!Number.isInteger(numericId) || numericId <= 0) {
    return NextResponse.json({ detail: "Invalid message id" }, { status: 400 });
  }

  let upstream: Response;
  try {
    upstream = await fetch(backendApiUrl(`/admin/messages/${numericId}`), {
      method: "DELETE",
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
    return NextResponse.json(payload || { detail: "Failed to delete message" }, { status: upstream.status });
  }

  return NextResponse.json(payload || { deleted_id: numericId });
}
