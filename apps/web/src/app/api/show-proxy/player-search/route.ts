import { NextRequest, NextResponse } from "next/server";

const DEFAULT_SHOW_SEARCH_URL = "https://mlb26.theshow.com/apis/player_search.json";
const SHOW_PROXY_SECRET_HEADER = "x-show-proxy-secret";

function showSearchUrl(): string {
  const configured = (process.env.SHOW_SEARCH_URL || "").trim();
  return configured || DEFAULT_SHOW_SEARCH_URL;
}

function sharedSecret(): string {
  return (process.env.SHOW_PROXY_SHARED_SECRET || "").trim();
}

export async function GET(request: NextRequest): Promise<NextResponse> {
  const expectedSecret = sharedSecret();
  if (!expectedSecret) {
    return NextResponse.json({ detail: "Show proxy is not configured" }, { status: 503 });
  }

  const providedSecret = (request.headers.get(SHOW_PROXY_SECRET_HEADER) || "").trim();
  if (!providedSecret || providedSecret !== expectedSecret) {
    return NextResponse.json({ detail: "Unauthorized" }, { status: 401 });
  }

  const username = (request.nextUrl.searchParams.get("username") || "").trim();
  if (!username) {
    return NextResponse.json({ detail: "Username is required" }, { status: 400 });
  }

  try {
    const upstream = new URL(showSearchUrl());
    upstream.searchParams.set("username", username);

    const response = await fetch(upstream.toString(), {
      method: "GET",
      cache: "no-store",
      headers: {
        Accept: "application/json, text/plain, */*",
      },
    });

    const text = await response.text();
    const headers = new Headers();
    headers.set("Cache-Control", "no-store");
    headers.set("Content-Type", response.headers.get("content-type") || "application/json");

    return new NextResponse(text, {
      status: response.status,
      headers,
    });
  } catch {
    return NextResponse.json({ detail: "Failed to reach The Show API" }, { status: 502 });
  }
}
