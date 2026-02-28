import { NextResponse } from "next/server";

import { backendApiUrl } from "@/lib/backend-api";

type BackendCardPreview = {
  id?: string;
  name?: string;
  ovr?: number;
  img?: string | null;
  baked_img?: string | null;
};

type HomeCardPreview = {
  id: string;
  name: string;
  ovr: number;
  image: string;
};

function sanitizeCard(row: BackendCardPreview): HomeCardPreview | null {
  if (typeof row.id !== "string" || row.id.length === 0) {
    return null;
  }
  if (typeof row.name !== "string" || row.name.length === 0) {
    return null;
  }
  if (typeof row.ovr !== "number" || Number.isNaN(row.ovr)) {
    return null;
  }

  return {
    id: row.id,
    name: row.name,
    ovr: row.ovr,
    image: typeof row.baked_img === "string" && row.baked_img.length > 0
      ? row.baked_img
      : typeof row.img === "string" && row.img.length > 0
        ? row.img
        : "",
  };
}

export async function GET() {
  try {
    const response = await fetch(
      backendApiUrl("/cards?series=live&rarity=diamond&year=25&limit=6"),
      { cache: "no-store" },
    );

    if (!response.ok) {
      return NextResponse.json([], { status: 200 });
    }

    const payload = (await response.json()) as BackendCardPreview[];
    if (!Array.isArray(payload)) {
      return NextResponse.json([], { status: 200 });
    }

    const cards = payload
      .map((row) => sanitizeCard(row))
      .filter((row): row is HomeCardPreview => row !== null);

    return NextResponse.json(cards, { status: 200 });
  } catch {
    return NextResponse.json([], { status: 200 });
  }
}
