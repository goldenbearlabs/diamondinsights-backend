import { NextResponse } from "next/server";

import {
  ADMIN_COOKIE_NAME,
  backendApiUrl,
  backendUnavailableResponse,
  encodeBasicAuth,
} from "@/lib/admin-api";

type LoginBody = {
  username?: string;
  password?: string;
};

export async function POST(request: Request) {
  let body: LoginBody;
  try {
    body = (await request.json()) as LoginBody;
  } catch {
    return NextResponse.json({ detail: "Invalid request body" }, { status: 400 });
  }

  const username = (body.username || "").trim();
  const password = body.password || "";
  if (!username || !password) {
    return NextResponse.json({ detail: "Username and password are required" }, { status: 400 });
  }

  const authorization = encodeBasicAuth(username, password);
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

  if (!upstream.ok) {
    return NextResponse.json({ detail: "Invalid credentials" }, { status: 401 });
  }

  const token = authorization.replace(/^Basic\s+/i, "");
  const response = NextResponse.json({ ok: true });
  response.cookies.set({
    name: ADMIN_COOKIE_NAME,
    value: token,
    path: "/",
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
    maxAge: 60 * 60 * 12,
  });
  return response;
}
