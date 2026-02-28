import { NextRequest, NextResponse } from "next/server";

import { backendApiUrl, backendUnavailableResponse } from "@/lib/backend-api";

function buildBackendPath(pathSegments: string[], search: string): string {
  const joined = pathSegments.join("/");
  const normalizedPath = `/users/${joined}`;
  return `${normalizedPath}${search || ""}`;
}

async function proxyRequest(
  request: NextRequest,
  method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE",
  params: { path: string[] },
): Promise<NextResponse> {
  try {
    const backendPath = buildBackendPath(params.path, request.nextUrl.search);

    const headers = new Headers();
    const authorization = request.headers.get("authorization");
    if (authorization) {
      headers.set("authorization", authorization);
    }
    const contentType = request.headers.get("content-type");
    if (contentType) {
      headers.set("content-type", contentType);
    }

    const body = method === "GET" ? undefined : await request.text();
    const response = await fetch(backendApiUrl(backendPath), {
      method,
      headers,
      body: body && body.length > 0 ? body : undefined,
      cache: "no-store",
    });

    const text = await response.text();
    const nextHeaders = new Headers();
    const responseContentType = response.headers.get("content-type");
    if (responseContentType) {
      nextHeaders.set("content-type", responseContentType);
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

export function GET(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  return context.params.then((params) => proxyRequest(request, "GET", params));
}

export function POST(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  return context.params.then((params) => proxyRequest(request, "POST", params));
}

export function PUT(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  return context.params.then((params) => proxyRequest(request, "PUT", params));
}

export function PATCH(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  return context.params.then((params) => proxyRequest(request, "PATCH", params));
}

export function DELETE(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  return context.params.then((params) => proxyRequest(request, "DELETE", params));
}
