from apps.jobs.router import Router
from shared.core.logging_config import configure_logging

def main():
    configure_logging(service_name="jobs")
    router = Router()
    router.run()

if __name__ == "__main__":
    main()
