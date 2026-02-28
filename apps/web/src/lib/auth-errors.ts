import type { FirebaseError } from "firebase/app";

import { ApiError } from "./api";

const FIREBASE_AUTH_MESSAGES: Record<string, string> = {
  "auth/email-already-in-use": "That email is already registered.",
  "auth/invalid-email": "Enter a valid email address.",
  "auth/invalid-credential": "Incorrect email or password.",
  "auth/missing-email": "Email is required.",
  "auth/missing-password": "Password is required.",
  "auth/network-request-failed": "Network error. Check your connection and try again.",
  "auth/operation-not-allowed": "This sign-in method is not enabled.",
  "auth/too-many-requests": "Too many attempts. Try again in a few minutes.",
  "auth/user-disabled": "This account has been disabled.",
  "auth/user-not-found": "No account exists for that email.",
  "auth/weak-password": "Password is too weak. Use at least 6 characters.",
  "auth/wrong-password": "Incorrect email or password.",
};

function parseApiErrorDetail(body: string): string | null {
  const trimmed = body.trim();
  if (!trimmed) {
    return null;
  }

  try {
    const parsed = JSON.parse(trimmed) as { detail?: unknown };
    if (typeof parsed.detail === "string" && parsed.detail.trim()) {
      return parsed.detail.trim();
    }
  } catch {
    // Fall through to raw text.
  }

  return trimmed;
}

function mapApiDetail(detail: string): string {
  const normalized = detail.toLowerCase();

  if (normalized.includes("display name already in use") || normalized.includes("username already in use")) {
    return "That username is already taken. Try another one.";
  }
  if (normalized.includes("email already in use")) {
    return "That email is already registered.";
  }
  if (normalized.includes("invalid or expired firebase token")) {
    return "Your session expired. Please sign in again.";
  }
  if (normalized.includes("missing authorization header")) {
    return "Please sign in to continue.";
  }
  if (normalized.includes("missing next_public_api_base_url")) {
    return "App configuration is missing. Set NEXT_PUBLIC_API_BASE_URL.";
  }

  return detail;
}

export function toReadableAuthError(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    const detail = parseApiErrorDetail(error.body);
    if (!detail) {
      return fallback;
    }
    return mapApiDetail(detail);
  }

  const code = (error as FirebaseError | undefined)?.code;
  if (code && FIREBASE_AUTH_MESSAGES[code]) {
    return FIREBASE_AUTH_MESSAGES[code];
  }

  const message = (error as { message?: unknown } | undefined)?.message;
  if (typeof message === "string" && message.trim()) {
    return message.trim();
  }

  return fallback;
}
