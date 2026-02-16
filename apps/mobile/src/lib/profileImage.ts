import { useCallback, useEffect, useState } from "react";
import { DeviceEventEmitter } from "react-native";
import { onAuthStateChanged } from "firebase/auth";
import { getDownloadURL, ref } from "firebase/storage";
import { Image } from "expo-image";

import { auth, storage } from "./firebase";


const urlCache = new Map<string, string>();
const inFlight = new Map<string, Promise<string | null>>();


export async function resolveAvatarUrl(
  pathOrUrl: string
): Promise<string | null> {
  if (!pathOrUrl) return null;

  if (pathOrUrl.startsWith("http")) return pathOrUrl;

  const cached = urlCache.get(pathOrUrl);
  if (cached) return cached;

  const pending = inFlight.get(pathOrUrl);
  if (pending) return pending;

  const promise = (async () => {
    try {
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
  return promise;
}


export function invalidateAvatarCache(path?: string) {
  if (path) {
    urlCache.delete(path);
  } else {
    urlCache.clear();
  }
  Image.clearMemoryCache();
  Image.clearDiskCache();
}


let cachedProfileUri: string | null = null;

export function useProfileImageUri() {
  const [profileUri, setProfileUri] = useState<string | null>(cachedProfileUri);
  const [uid, setUid] = useState<string | null>(auth.currentUser?.uid ?? null);

  const updateProfileUri = useCallback((nextUri: string | null) => {
    cachedProfileUri = nextUri;
    setProfileUri(nextUri);
  }, []);

  useEffect(() => {
    const unsub = onAuthStateChanged(auth, (user) => {
      setUid(user?.uid ?? null);
      if (!user) updateProfileUri(null);
    });
    return unsub;
  }, [updateProfileUri]);

  useEffect(() => {
    let active = true;

    const loadProfileImage = async () => {
      if (!uid) {
        if (active) updateProfileUri(null);
        return;
      }
      try {
        const path = `users/${uid}/profile.jpg`;
        const url = await resolveAvatarUrl(path);
        if (active) updateProfileUri(url);
      } catch {
        if (active) updateProfileUri(null);
      }
    };

    loadProfileImage();
    const sub = DeviceEventEmitter.addListener("profile-image-updated", () => {
      const path = uid ? `users/${uid}/profile.jpg` : null;
      if (path) invalidateAvatarCache(path);
      loadProfileImage();
    });

    return () => {
      active = false;
      sub.remove();
    };
  }, [uid, updateProfileUri]);

  return { profileUri, setProfileUri: updateProfileUri };
}
