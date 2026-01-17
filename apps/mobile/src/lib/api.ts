import { API_BASE_URL } from "./config";

export async function apiGet<T>(path: string): Promise<T> {
  if (!API_BASE_URL) throw new Error("Missing EXPO_PUBLIC_API_BASE_URL");
  const url = `${API_BASE_URL}${path}`;
  try {
    console.log("[apiGet] GET", url);
    const res = await fetch(url);
    console.log("[apiGet] status", res.status, res.ok);
    const text = await res.text();
    console.log("[apiGet] body", text ? text.slice(0, 500) : "<empty>");
    if (!res.ok) throw new Error(text || `HTTP ${res.status}`);
    return (text ? JSON.parse(text) : {}) as T;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.log("[apiGet] error", message);
    throw err;
  }
}
