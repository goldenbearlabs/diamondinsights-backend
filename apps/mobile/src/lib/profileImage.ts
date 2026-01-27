import { useCallback, useEffect, useState } from "react";
import { DeviceEventEmitter } from "react-native";
import { onAuthStateChanged } from "firebase/auth";
import { getDownloadURL, ref } from "firebase/storage";

import { auth, storage } from "./firebase";

export function withCacheBuster(url: string, cacheKey: string | number = Date.now()) {
  const separator = url.includes("?") ? "&" : "?";
  return `${url}${separator}t=${cacheKey}`;
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
        const url = await getDownloadURL(ref(storage, `users/${uid}/profile.jpg`));
        if (active) updateProfileUri(withCacheBuster(url));
      } catch {
        if (active) updateProfileUri(null);
      }
    };

    loadProfileImage();
    const sub = DeviceEventEmitter.addListener("profile-image-updated", loadProfileImage);

    return () => {
      active = false;
      sub.remove();
    };
  }, [uid, updateProfileUri]);

  return { profileUri, setProfileUri: updateProfileUri };
}
