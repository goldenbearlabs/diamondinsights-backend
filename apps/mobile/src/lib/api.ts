import { API_BASE_URL } from "./config";
import { auth } from "./firebase";

async function getIdTokenOrNull() {
  const user = auth.currentUser;
  if (!user) return null;
  return user.getIdToken();
}

export class ApiError extends Error {
  status: number;
  body: string;

  constructor(status: number, body: string) {
    super(body || `HTTP ${status}`);
    this.status = status;
    this.body = body;
  }
}

async function apiRequest<T>(
  method: string,
  path: string,
  body?: unknown,
  opts?: { auth?: boolean; headers?: Record<string, string> }
): Promise<T> {
  if (!API_BASE_URL) throw new Error("Missing EXPO_PUBLIC_API_BASE_URL");

  const url = `${API_BASE_URL}${path}`;
  const wantAuth = opts?.auth ?? false;

  const headers: Record<string, string> = { ...(opts?.headers || {}) };
  if (body !== undefined) headers["Content-Type"] = "application/json";

  if (wantAuth) {
    const token = await getIdTokenOrNull();
    if (!token) throw new Error("Not authenticated");
    headers["Authorization"] = `Bearer ${token}`;
  }

  const res = await fetch(url, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });

  const text = await res.text();

  if (!res.ok) throw new ApiError(res.status, text);

  return (text ? JSON.parse(text) : {}) as T;
}

export async function apiGet<T>(path: string): Promise<T> {
  return apiRequest<T>("GET", path);
}

export async function apiGetAuth<T>(path: string): Promise<T> {
  return apiRequest<T>("GET", path, undefined, { auth: true });
}

export async function apiPost<T>(path: string, body?: unknown): Promise<T> {
  return apiRequest<T>("POST", path, body);
}

export async function apiPostAuth<T>(path: string, body?: unknown): Promise<T> {
  return apiRequest<T>("POST", path, body, { auth: true });
}

export async function apiPutAuth<T>(path: string, body?: unknown): Promise<T> {
  return apiRequest<T>("PUT", path, body, { auth: true });
}

export async function apiPatchAuth<T>(path: string, body?: unknown): Promise<T> {
  return apiRequest<T>("PATCH", path, body, { auth: true });
}

export async function apiDeleteAuth<T>(path: string): Promise<T> {
  return apiRequest<T>("DELETE", path, undefined, { auth: true });
}



export interface UserPredictionCreate {
  card_id: string;
  predicted_ovr: number;
}

export interface UserPredictionResponse {
  user_id: number;
  card_id: string;
  predicted_ovr: number;
}

export function getUserPrediction(cardId: string) {
  return apiRequest<UserPredictionResponse>("GET", `/user-predictions/${cardId}`, undefined, { auth: true });
}

export function saveUserPrediction(body: UserPredictionCreate) {
  return apiRequest<UserPredictionResponse>("POST", "/user-predictions/", body, { auth: true });
}