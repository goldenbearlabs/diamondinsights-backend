from abc import ABC, abstractmethod
from src.database.database import SessionLocal
from src.core.http_client import APIClient
import logging
import sys

class BaseJob(ABC):
    def __init__(self):
        self.child_instance = None
        self.api_client = APIClient()
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )

        self.logger = logging.getLogger(__name__)

    def set_child_instance(self, child_obj):
        self.child_instance = child_obj

    def get_child_class_name(self):
        if self.child_instance:
            return self.child_instance.__class__.__name__
        else:
            return "No child instance set"

    @abstractmethod
    def execute(self, db_session):
        """Logic for the job goes here."""
        pass

    def run(self):
        """The entry point that handles the DB session lifecycle."""
        self.logger.info(f"Starting job... {self.child_instance.__class__.__name__}")
        db = SessionLocal()
        try:
            self.execute(db)
            db.commit()
            self.logger.info("Job completed successfully.")
        except Exception as e:
            db.rollback()
            self.logger.error(f"Job failed: {e}", exc_info=True)
        finally:
            db.close()

    def _json_get(self, json, key, default=None):
        "Safe json get"
        if not json:
            return default
        if key not in json:
            return default
        return json[key]