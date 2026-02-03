from __future__ import annotations

import os
import firebase_admin
from firebase_admin import credentials


def init_firebase_admin() -> None:
    if firebase_admin._apps:
        return

    sa_path = os.getenv("FIREBASE_SERVICE_ACCOUNT_PATH")
    if not sa_path:
        raise RuntimeError("Missing FIREBASE_SERVICE_ACCOUNT_PATH")

    cred = credentials.Certificate(sa_path)
    firebase_admin.initialize_app(cred)
