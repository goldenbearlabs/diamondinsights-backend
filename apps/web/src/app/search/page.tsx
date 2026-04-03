import SearchPageClient from "./SearchPageClient";

export default async function SearchPage({
  searchParams,
}: {
  searchParams: Promise<{ q?: string; mode?: string }>;
}) {
  const params = await searchParams;
  const initialQuery = typeof params.q === "string" ? params.q : "";
  const initialMode = typeof params.mode === "string" ? params.mode : undefined;

  return <SearchPageClient key={`${initialMode ?? "showprofiles"}:${initialQuery}`} initialQuery={initialQuery} initialMode={initialMode} />;
}
