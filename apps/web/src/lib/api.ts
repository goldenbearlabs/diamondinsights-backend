import { getFirebaseAuth } from "./firebase";

async function getIdTokenOrNull() {
  const auth = getFirebaseAuth();
  const user = auth.currentUser;
  if (!user) {
    return null;
  }
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
  opts?: { auth?: boolean; headers?: Record<string, string> },
): Promise<T> {
  const url = `/api${path}`;
  const wantAuth = opts?.auth ?? false;

  const headers: Record<string, string> = { ...(opts?.headers || {}) };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
  }

  if (wantAuth) {
    const token = await getIdTokenOrNull();
    if (!token) {
      throw new Error("Not authenticated");
    }
    headers.Authorization = `Bearer ${token}`;
  }

  const response = await fetch(url, {
    method,
    headers,
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await response.text();

  if (!response.ok) {
    throw new ApiError(response.status, text);
  }

  return (text ? JSON.parse(text) : {}) as T;
}

export function apiGet<T>(path: string): Promise<T> {
  return apiRequest<T>("GET", path);
}

export function apiGetAuth<T>(path: string): Promise<T> {
  return apiRequest<T>("GET", path, undefined, { auth: true });
}

export function apiPostAuth<T>(path: string, body?: unknown): Promise<T> {
  return apiRequest<T>("POST", path, body, { auth: true });
}

export function apiPutAuth<T>(path: string, body?: unknown): Promise<T> {
  return apiRequest<T>("PUT", path, body, { auth: true });
}

export function apiDeleteAuth<T>(path: string): Promise<T> {
  return apiRequest<T>("DELETE", path, undefined, { auth: true });
}

export interface DisplayNameAvailability {
  available: boolean;
}

export function getDisplayNameAvailability(displayName: string) {
  const query = encodeURIComponent(displayName.trim());
  return apiGet<DisplayNameAvailability>(`/users/display-name-available?display_name=${query}`);
}

export interface EntitlementRecord {
  entitlement_id: string;
  is_active: boolean;
  product_identifier: string | null;
  store: string | null;
  environment: string | null;
  expires_at: string | null;
  updated_at: string;
}

export interface EntitlementsMeResponse {
  has_pro: boolean;
  pro_entitlement_id: string;
  pro_expires_at: string | null;
  entitlements: EntitlementRecord[];
}

export function getMyEntitlements() {
  return apiRequest<EntitlementsMeResponse>("GET", "/entitlements/me", undefined, { auth: true });
}
