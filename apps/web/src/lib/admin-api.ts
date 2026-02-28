import { NextRequest, NextResponse } from "next/server";

export const ADMIN_COOKIE_NAME = "di_admin_auth";

export function encodeBasicAuth(username: string, password: string): string {
  return `Basic ${Buffer.from(`${username}:${password}`).toString("base64")}`;
}

export function authHeaderFromCookie(request: NextRequest): string | null {
  const token = request.cookies.get(ADMIN_COOKIE_NAME)?.value;
  if (!token) {
    return null;
  }
  return `Basic ${token}`;
}

export function adminUnauthorizedResponse(message = "Unauthorized"): NextResponse {
  const response = NextResponse.json({ detail: message }, { status: 401 });
  response.cookies.set({
    name: ADMIN_COOKIE_NAME,
    value: "",
    path: "/",
    maxAge: 0,
    httpOnly: true,
    sameSite: "lax",
    secure: process.env.NODE_ENV === "production",
  });
  return response;
}
