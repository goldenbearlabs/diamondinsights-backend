# Mobile App Architecture

This document describes the structure and responsibilities of the Expo React Native app in `apps/mobile`.

## Stack and entry points
- Expo + React Native with Expo Router for file-based routing.
- `index.js` registers the Expo Router root and points it at the `app/` directory.
- `app/_layout.tsx` is the global layout that gates routes based on Firebase auth state.

## Folder map (high level)
```
apps/mobile/
  app/                 # Expo Router routes and layouts
  src/                 # Feature screens, shared UI, services, and theme
  assets/              # App icons and image assets
  project/             # Project docs (this file)
  app.json             # Expo configuration
  index.js             # App entry (registers Expo Router)
  package.json         # App dependencies and scripts
  tsconfig.json        # TypeScript config
  babel.config.js      # Babel config
  eslint.config.js     # Lint rules
  expo-env.d.ts        # Expo TS types
```

## Routing and navigation (`app/`)
Expo Router uses filesystem-based routes. The `app/` folder is the only place
routes are defined, and most screens simply re-export implementations from `src/`.

- `app/_layout.tsx`
  - Root layout that listens to Firebase auth changes.
  - Redirects unauthenticated users to `/(auth)/signin`.
  - Redirects authenticated users to `/(app)` (tabbed area).
- `app/(auth)/signin.tsx` and `app/(auth)/signup.tsx`
  - Auth group, re-export the screen implementations from `src/screens/`.
- `app/(app)/_layout.tsx`
  - Tab layout (Home, Predictions, Portfolio, Chat, Explore).
  - Styling and icons for the bottom tab bar live here.
- `app/(app)/index.tsx`, `predictions.tsx`, `portfolio.tsx`, `chat.tsx`, `explore.tsx`
  - Home re-exports `src/screens/home/HomeScreen`.
  - Other tabs are placeholder screens for now.

## Screens and UI (`src/screens`, `src/components`, `src/homescreencomponents`)
- `src/screens/SignInScreen.tsx`
  - Firebase email/password sign-in.
  - Calls backend `GET /users/me` and conditionally `POST /users/signup`.
- `src/screens/SignUpScreen.tsx`
  - Firebase sign-up, profile update, optional image pick + upload.
  - Calls backend `POST /users/signup`.
- `src/screens/home/HomeScreen.tsx`
  - Home experience built from reusable components and a scrolling layout.
  - Uses `FloatingBackground`, `PredictionCarousel`, `TrustStats`, `HowItWorks`, `ContactCard`.
- `src/components/HeaderBar.tsx`
  - Reusable header bar with optional right-side element.
- `src/homescreencomponents/*`
  - `FloatingBackground`: animated background particles using shield images.
  - `PredictionCarousel`: API-backed carousel of cards with animated scaling.
  - `TrustStats`, `HowItWorks`, `ContactCard`: marketing/info panels for the home screen.

## Services and data access (`src/lib`)
- `src/lib/config.ts`
  - Reads `EXPO_PUBLIC_API_BASE_URL` for backend calls.
- `src/lib/api.ts`
  - Fetch wrapper with optional Firebase ID token auth.
  - Helpers for GET/POST/PUT/DELETE with or without auth.
- `src/lib/firebase.ts`
  - Firebase app init, Auth (with AsyncStorage persistence), and Storage.
  - Uses `EXPO_PUBLIC_FIREBASE_*` env vars for configuration.
- `src/lib/storage.ts`
  - Uploads profile images to Firebase Storage at `users/{uid}/profile.jpg`.

## Theme and styling (`src/theme`)
- `src/theme/colors.ts`
  - Central palette and spacing object used by components.
- `src/theme/tokens.ts`, `src/theme/index.ts`, `src/theme/ThemeProvider.tsx`
  - Tokenized theme system with light/dark variants and a context provider.
  - Can be adopted by screens for consistent theming.

## Types (`src/types`)
- `src/types/firebase-auth.d.ts`
  - Extends Firebase auth types for React Native persistence.

## Assets (`assets/`)
- `assets/images/`
  - App icon, placeholder, and shield images used in the UI and backgrounds.

## Configuration and tooling
- `app.json`
  - Expo config: app metadata, icons, splash screen, router plugin, typed routes.
  - Enables the new architecture and React compiler experiment.
- `package.json`
  - Dependencies: Expo, Expo Router, Firebase, React Navigation, etc.
  - Scripts for running on iOS/Android/web and linting.

## Environment variables
These are required at runtime (typically defined in `.env` files or CI secrets):
- `EXPO_PUBLIC_API_BASE_URL`
- `EXPO_PUBLIC_FIREBASE_API_KEY`
- `EXPO_PUBLIC_FIREBASE_AUTH_DOMAIN`
- `EXPO_PUBLIC_FIREBASE_PROJECT_ID`
- `EXPO_PUBLIC_FIREBASE_STORAGE_BUCKET`
- `EXPO_PUBLIC_FIREBASE_MESSAGING_SENDER_ID`
- `EXPO_PUBLIC_FIREBASE_APP_ID`

## Data flow summary
- Authentication: Firebase Auth drives route access in `app/_layout.tsx`.
- Backend access: `src/lib/api.ts` attaches Firebase ID tokens for protected endpoints.
- Storage: Profile images upload to Firebase Storage using `src/lib/storage.ts`.
