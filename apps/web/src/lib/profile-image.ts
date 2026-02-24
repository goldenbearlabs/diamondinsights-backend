import { getDownloadURL, ref } from "firebase/storage";

import { getFirebaseStorage } from "./firebase";

const urlCache = new Map<string, string>();
const inFlight = new Map<string, Promise<string | null>>();

export const PROFILE_IMAGE_UPDATED_EVENT = "di:profile-image-updated";

type ResolveAvatarOptions = {
  bustCache?: boolean;
};

function withCacheBust(url: string): string {
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}v=${Date.now()}`;
}

export async function resolveAvatarUrl(
  pathOrUrl: string | null | undefined,
  options?: ResolveAvatarOptions,
): Promise<string | null> {
  if (!pathOrUrl) {
    return null;
  }

  const bustCache = options?.bustCache ?? false;

  if (pathOrUrl.startsWith("http://") || pathOrUrl.startsWith("https://")) {
    return bustCache ? withCacheBust(pathOrUrl) : pathOrUrl;
  }

  if (bustCache) {
    urlCache.delete(pathOrUrl);
  }

  const cached = urlCache.get(pathOrUrl);
  if (cached) {
    return bustCache ? withCacheBust(cached) : cached;
  }

  const pending = inFlight.get(pathOrUrl);
  if (pending) {
    const pendingResult = await pending;
    return pendingResult && bustCache ? withCacheBust(pendingResult) : pendingResult;
  }

  const promise = (async () => {
    try {
      const storage = getFirebaseStorage();
      const url = await getDownloadURL(ref(storage, pathOrUrl));
      urlCache.set(pathOrUrl, url);
      return url;
    } catch {
      return null;
    } finally {
      inFlight.delete(pathOrUrl);
    }
  })();

  inFlight.set(pathOrUrl, promise);
  const result = await promise;
  return result && bustCache ? withCacheBust(result) : result;
}

export function invalidateAvatarCache(path?: string) {
  if (path) {
    urlCache.delete(path);
    return;
  }
  urlCache.clear();
}

export function emitProfileImageUpdated(path?: string) {
  if (typeof window === "undefined") {
    return;
  }
  window.dispatchEvent(new CustomEvent(PROFILE_IMAGE_UPDATED_EVENT, { detail: { path } }));
}
