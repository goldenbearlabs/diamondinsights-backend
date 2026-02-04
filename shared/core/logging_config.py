import logging
import os

_BASE_FACTORY = logging.getLogRecordFactory()
_CURRENT_SERVICE: str | None = None


def _record_factory(*args, **kwargs) -> logging.LogRecord:
    record = _BASE_FACTORY(*args, **kwargs)
    if not hasattr(record, "service"):
        record.service = _CURRENT_SERVICE or "unknown"
    return record


def configure_logging(service_name: str | None = None, level: str | None = None) -> None:
    global _CURRENT_SERVICE
    if service_name:
        _CURRENT_SERVICE = service_name

    logging.setLogRecordFactory(_record_factory)

    level_name = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    level_value = logging.getLevelName(level_name)
    if isinstance(level_value, str):
        level_value = logging.INFO

    if service_name:
        fmt = "%(asctime)s %(levelname)s %(name)s [service=%(service)s] - %(message)s"
        logging.basicConfig(level=level_value, format=fmt)
    else:
        fmt = "%(asctime)s %(levelname)s %(name)s - %(message)s"
        logging.basicConfig(level=level_value, format=fmt)
