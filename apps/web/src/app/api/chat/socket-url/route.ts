import { NextResponse } from "next/server";

import { backendApiUrl } from "@/lib/backend-api";

export async function GET() {
  const wsUrl = backendApiUrl("/ws/chat").replace(/^http/i, "ws");
  return NextResponse.json({ url: wsUrl });
}
