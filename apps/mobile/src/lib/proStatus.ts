import { useCallback, useEffect, useState } from "react";

import { getMyEntitlements } from "./api";

const CACHE_TTL_MS = 15_000;

let cachedHasPro: boolean | null = null;
let cachedAtMs = 0;
let inFlight: Promise<boolean> | null = null;

const listeners = new Set<(value: boolean | null) => void>();

const notifyListeners = () => {
  for (const listener of listeners) {
    listener(cachedHasPro);
  }
};

const coerceErrorMessage = (err: unknown): string => {
  if (err instanceof Error && err.message) return err.message;
  if (typeof err === "string" && err.trim()) return err.trim();
  return "Failed to load pro status.";
};

const subscribe = (listener: (value: boolean | null) => void) => {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
};

export const getCachedBackendProStatus = (): boolean | null => cachedHasPro;

export const setBackendProStatus = (hasPro: boolean): void => {
  cachedHasPro = hasPro;
  cachedAtMs = Date.now();
  notifyListeners();
};

export const clearBackendProStatus = (): void => {
  cachedHasPro = null;
  cachedAtMs = 0;
  notifyListeners();
};

export async function refreshBackendProStatus(force = false): Promise<boolean> {
  const now = Date.now();
  if (!force && cachedHasPro !== null && now - cachedAtMs < CACHE_TTL_MS) {
    return cachedHasPro;
  }

  if (inFlight) return inFlight;

  inFlight = (async () => {
    const data = await getMyEntitlements();
    const hasPro = Boolean(data.has_pro);
    setBackendProStatus(hasPro);
    return hasPro;
  })().finally(() => {
    inFlight = null;
  });

  return inFlight;
}

export function useBackendProStatus() {
  const [isPro, setIsPro] = useState<boolean | null>(getCachedBackendProStatus());
  const [loading, setLoading] = useState<boolean>(getCachedBackendProStatus() === null);
  const [error, setError] = useState<string | null>(null);

  const refresh = useCallback(async (force = false): Promise<boolean | null> => {
    setLoading(true);
    try {
      const next = await refreshBackendProStatus(force);
      setIsPro(next);
      setError(null);
      return next;
    } catch (err: unknown) {
      setError(coerceErrorMessage(err));
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    const unsubscribe = subscribe((value) => {
      setIsPro(value);
    });

    void refresh(false);

    return unsubscribe;
  }, [refresh]);

  return { isPro, loading, error, refresh };
}

