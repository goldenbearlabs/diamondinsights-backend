import firebase_admin
from firebase_admin import credentials, App
import logging

logger = logging.getLogger(__name__)

def init_firebase_admin() -> App:
    """Initialize (or fetch) the default Firebase Admin app for this process.

    Intended to run during FastAPI startup. This function is idempotent with a single Python process:
    if the default app already exists, it is returned.

    Runtime Requirements: 
        A service account JSON must be available at:
        /run/secrets/firebase_service_key.json

    Returns:
        Returns the current instance the Firebase App class

    Raises:
        IOError/OSError:
            If the service account file cannot be read. :contentReference[oaicite:2]{index=2}
        ValueError:
            If the certificate content is invalid, or Firebase initialization
            fails due to invalid arguments. :contentReference[oaicite:3]{index=3}
    """
    FIREBASE_SERVICE_ACCOUNT_PATH = "/run/secrets/firebase_service_key.json"

    # Early return if firebase admin is already initialized
    # Firebase API doesn't provide a clean approach to do this unfortunately
    try:
        return firebase_admin.get_app()
    except ValueError:
        pass
    
    try:
        cred = credentials.Certificate(FIREBASE_SERVICE_ACCOUNT_PATH)
    except IOError:
        logger.error("Missing FIREBASE_SERVICE_ACCOUNT_PATH. /run/secrets/firebase_service_key.json is required at runtime")
        raise
    return firebase_admin.initialize_app(cred)
