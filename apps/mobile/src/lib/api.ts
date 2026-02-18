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

// ── Prediction Leaderboard ──────────────────────────────────────────

export interface LeaderboardEntry {
  rank: number;
  user_id: number;
  display_name: string;
  profile_img_path: string;
  prediction_count: number;
  score: number | null;
}

export interface PredictionLeaderboardResponse {
  items: LeaderboardEntry[];
  total_participants: number;
  my_rank: number | null;
  my_prediction_count: number | null;
}

export function getPredictionLeaderboard() {
  return apiRequest<PredictionLeaderboardResponse>("GET", "/user-predictions/leaderboard", undefined, { auth: true });
}

// ── Portfolio Holdings ──────────────────────────────────────────────

export interface UpdateHoldingBody {
  quantity?: number;
  avg_price?: number | null;
  user_predicted_ovr?: number | null;
  notes?: string | null;
}

export function updatePortfolioHolding(cardId: string, body: UpdateHoldingBody) {
  return apiRequest<void>("PUT", `/portfolios/me/holdings/${cardId}`, body, { auth: true });
}

// ── Flipping ───────────────────────────────────────────────────────

export type FlippingSortBy =
  | "profit"
  | "spread"
  | "profit_per_min"
  | "margin"
  | "orders"
  | "buys"
  | "sells"
  | "buys_sells"
  | "buy"
  | "sell"
  | "ovr"
  | "name";

export interface FlippingRow {
  card_id: string;
  name: string | null;
  team: string | null;
  ovr: number;
  series: string | null;
  year: number | null;
  baked_img: string | null;
  best_sell_price: number;
  best_buy_price: number;
  effective_buy_price: number;
  quicksell_price: number;
  uses_quicksell_buy: boolean;
  after_tax_sell_price: number;
  spread: number;
  profit: number;
  profit_margin_pct: number | null;
  orders_1h: number;
  buys_1h: number;
  sells_1h: number;
  avg_completed_price_1h: number | null;
  latest_completed_order_at: string | null;
}

export interface GetFlippingRowsParams {
  limit?: number;
  offset?: number;
  sort_by?: FlippingSortBy;
  sort_dir?: "asc" | "desc";
  profitable_only?: boolean;
  min_buy?: number;
  max_buy?: number;
  min_sell?: number;
  max_sell?: number;
  min_ovr?: number;
  max_ovr?: number;
  series?: string;
  name?: string;
}

export function getFlippingRows(params: GetFlippingRowsParams = {}) {
  const query = new URLSearchParams();

  if (params.limit != null) query.set("limit", String(params.limit));
  if (params.offset != null) query.set("offset", String(params.offset));
  if (params.sort_by) query.set("sort_by", params.sort_by);
  if (params.sort_dir) query.set("sort_dir", params.sort_dir);
  if (params.profitable_only != null) {
    query.set("profitable_only", params.profitable_only ? "true" : "false");
  }
  if (params.min_buy != null) query.set("min_buy", String(params.min_buy));
  if (params.max_buy != null) query.set("max_buy", String(params.max_buy));
  if (params.min_sell != null) query.set("min_sell", String(params.min_sell));
  if (params.max_sell != null) query.set("max_sell", String(params.max_sell));
  if (params.min_ovr != null) query.set("min_ovr", String(params.min_ovr));
  if (params.max_ovr != null) query.set("max_ovr", String(params.max_ovr));
  if (params.series) query.set("series", params.series);
  if (params.name) query.set("name", params.name);

  const search = query.toString();
  return apiGet<FlippingRow[]>(`/flipping${search ? `?${search}` : ""}`);
}
