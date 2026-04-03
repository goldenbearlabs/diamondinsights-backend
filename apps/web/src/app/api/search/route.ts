import { NextRequest, NextResponse } from "next/server";

import { backendApiUrl, backendUnavailableResponse } from "@/lib/backend-api";

export async function GET(request: NextRequest): Promise<NextResponse> {
  try {
    const authorization = request.headers.get("authorization");
    const headers = new Headers();
    if (authorization) {
      headers.set("authorization", authorization);
    }

    const response = await fetch(backendApiUrl(`/search${request.nextUrl.search || ""}`), {
      method: "GET",
      headers,
      cache: "no-store",
    });

    const text = await response.text();
    const nextHeaders = new Headers();
    const contentType = response.headers.get("content-type");
    if (contentType) {
      nextHeaders.set("content-type", contentType);
    } else {
      nextHeaders.set("content-type", "application/json");
    }

    return new NextResponse(text, {
      status: response.status,
      headers: nextHeaders,
    });
  } catch {
    return backendUnavailableResponse();
  }
}
