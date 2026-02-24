# DI Web (Next.js)

This app lives at `apps/web` and will be deployed to Vercel.

## Local development

1. Use Node `20.11.1` (or any version `>=20.9.0`).
2. If you use `nvm`, run `nvm use`.
3. From repo root:

```bash
npm run web:dev
```

Then open `http://localhost:3000`.

## Build and lint

From repo root:

```bash
npm run web:build
npm run web:lint
```

## Vercel setup (monorepo)

From repo root:

```bash
npm run vercel:link:web
npm run vercel:pull:web
```

If Vercel asks for project settings, use:

- Framework preset: `Next.js`
- Root directory: `apps/web`
- Build command: `npm run build`
- Install command: `npm install`
- Output directory: `.next` (default)

Deploy commands:

```bash
npm run vercel:deploy:web
npm run vercel:deploy:web:prod
```

## Environment

- `BACKEND_API_URL`: base URL for the FastAPI backend (used by admin API proxy routes).
  - Local default is `http://localhost:8000`.
- User-facing web auth calls are proxied through Next route handlers at `/api/users/*` (same-origin).
- `BACKEND_API_URL` is used by these proxies to reach FastAPI.
- `NEXT_PUBLIC_FIREBASE_API_KEY`
- `NEXT_PUBLIC_FIREBASE_AUTH_DOMAIN`
- `NEXT_PUBLIC_FIREBASE_PROJECT_ID`
- `NEXT_PUBLIC_FIREBASE_STORAGE_BUCKET`
- `NEXT_PUBLIC_FIREBASE_MESSAGING_SENDER_ID`
- `NEXT_PUBLIC_FIREBASE_APP_ID`

## Notes

- Keep the user-facing web app and `/admin` under this same Next.js project.
- Route handlers can proxy admin actions to backend admin endpoints.
