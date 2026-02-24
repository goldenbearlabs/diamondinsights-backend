import { FirebaseApp, getApp, getApps, initializeApp } from "firebase/app";
import { Auth, getAuth } from "firebase/auth";
import { FirebaseStorage, getStorage } from "firebase/storage";

const REQUIRED_FIREBASE_ENV_KEYS = [
  "NEXT_PUBLIC_FIREBASE_API_KEY",
  "NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN",
  "NEXT_PUBLIC_FIREBASE_PROJECT_ID",
  "NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET",
  "NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID",
  "NEXT_PUBLIC_FIREBASE_APP_ID",
] as const;

const firebaseEnv = {
  NEXT_PUBLIC_FIREBASE_API_KEY: process.env.NEXT_PUBLIC_FIREBASE_API_KEY,
  NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN: process.env.NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN,
  NEXT_PUBLIC_FIREBASE_PROJECT_ID: process.env.NEXT_PUBLIC_FIREBASE_PROJECT_ID,
  NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET: process.env.NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET,
  NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID: process.env.NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID,
  NEXT_PUBLIC_FIREBASE_APP_ID: process.env.NEXT_PUBLIC_FIREBASE_APP_ID,
} as const;

let appCache: FirebaseApp | null = null;
let authCache: Auth | null = null;
let storageCache: FirebaseStorage | null = null;

function getMissingFirebaseEnvKeys(): string[] {
  return REQUIRED_FIREBASE_ENV_KEYS.filter((key) => {
    const value = firebaseEnv[key];
    return typeof value !== "string" || value.trim().length === 0;
  });
}

function buildFirebaseConfig() {
  const missing = getMissingFirebaseEnvKeys();
  if (missing.length > 0) {
    throw new Error(`Missing Firebase config: ${missing.join(", ")}`);
  }

  return {
    apiKey: firebaseEnv.NEXT_PUBLIC_FIREBASE_API_KEY as string,
    authDomain: firebaseEnv.NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN as string,
    projectId: firebaseEnv.NEXT_PUBLIC_FIREBASE_PROJECT_ID as string,
    storageBucket: firebaseEnv.NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET as string,
    messagingSenderId: firebaseEnv.NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID as string,
    appId: firebaseEnv.NEXT_PUBLIC_FIREBASE_APP_ID as string,
  };
}

export function getFirebaseApp(): FirebaseApp {
  if (appCache) {
    return appCache;
  }

  const config = buildFirebaseConfig();
  appCache = getApps().length > 0 ? getApp() : initializeApp(config);
  return appCache;
}

export function getFirebaseAuth(): Auth {
  if (authCache) {
    return authCache;
  }
  authCache = getAuth(getFirebaseApp());
  return authCache;
}

export function getFirebaseStorage(): FirebaseStorage {
  if (storageCache) {
    return storageCache;
  }
  storageCache = getStorage(getFirebaseApp());
  return storageCache;
}
