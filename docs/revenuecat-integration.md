# RevenueCat Integration Flow

This document explains the RevenueCat setup that was added and what still needs to be configured/run.

## What is implemented

### 1) Mobile app (Expo / React Native)

- RevenueCat SDK is initialized with `EXPO_PUBLIC_RC_API_KEY`.
- On auth state change, the app syncs RevenueCat user identity to the Firebase UID.
- Account screen includes a **Diamond Pro** section:
  - Reads entitlement state from backend (`GET /entitlements/me`)
  - Opens RevenueCat paywall (`presentPaywallIfNeeded`) for the configured entitlement
  - Refreshes entitlement state after purchase/restore

Relevant files:
- `apps/mobile/src/lib/revenuecat.ts`
- `apps/mobile/src/lib/config.ts`
- `apps/mobile/src/lib/api.ts`
- `apps/mobile/app/_layout.tsx`
- `apps/mobile/app/(app)/account.tsx`

### 2) Backend entitlement storage

Added DB models:
- `user_entitlements`: source-of-truth for active/inactive entitlements per user
- `revenuecat_webhook_events`: raw webhook audit + idempotency tracking

Relevant files:
- `shared/db/models.py`
- `apps/backend/alembic/versions/202602181430_add_revenuecat_entitlements.py`

### 3) Backend API routes

- `GET /entitlements/me`
  - Auth required (Firebase token)
  - Returns all entitlements for current user and `has_pro` convenience flag

- `POST /billing/revenuecat/webhook`
  - Consumes RevenueCat webhook payload
  - Idempotent by RevenueCat event ID
  - Resolves Firebase user from `app_user_id`, `original_app_user_id`, aliases
  - Upserts entitlement state in `user_entitlements`
  - Handles transfer events by deactivating source users

Relevant files:
- `apps/backend/src/api/routes/entitlements.py`
- `apps/backend/src/api/main.py`

## Environment variables

### Backend (`.env` on server/local backend)

- `REVENUECAT_WEBHOOK_AUTH=<secret-token>`
  - Used to validate webhook auth header
- `REVENUECAT_PRO_ENTITLEMENT_ID=pro`
  - Entitlement ID used for `has_pro` checks

Also added to:
- `.env.example`

### Mobile (`apps/mobile/.env`)

- `EXPO_PUBLIC_RC_API_KEY=<revenuecat_public_sdk_key>`
- `EXPO_PUBLIC_RC_PRO_ENTITLEMENT_ID=pro` (optional; defaults to `pro`)

## What you still need to do

1. Apply database migration
- Local docker/dev:
  - `make migrate`
- Production:
  - Run your normal Alembic upgrade flow to head.

2. Configure RevenueCat dashboard
- Create/confirm entitlement ID (expected default: `pro`)
- Configure paywall/offering in RevenueCat UI
- Configure webhook endpoint to your backend URL:
  - `https://<api-domain>/billing/revenuecat/webhook`
- Set webhook auth token in RevenueCat to match `REVENUECAT_WEBHOOK_AUTH`

3. Configure backend deployment env (DigitalOcean)
- Set `REVENUECAT_WEBHOOK_AUTH`
- Set `REVENUECAT_PRO_ENTITLEMENT_ID` (if not `pro`)
- Restart backend service

4. Confirm mobile env
- Ensure `EXPO_PUBLIC_RC_API_KEY` is set in `apps/mobile/.env` and in EAS env for cloud builds.

5. End-to-end test (test store)
- Sign in with a real Firebase user
- Open Account tab, tap **Unlock Pro**
- Complete test purchase
- Verify webhook hits backend and `GET /entitlements/me` returns `has_pro: true`

## Recommended deployment shape

- Use backend domain + HTTPS for webhook target (recommended over raw IP).
- Keep webhook target stable in development/testing (typically your DO backend), even if mobile app runs locally.

