import logging
import os
from typing import Optional

_BASE_FACTORY = logging.getLogRecordFactory()
_CURRENT_SERVICE: Optional[str] = None


def _record_factory(*args, **kwargs) -> logging.LogRecord:
    """LogRecord factory that injects a `service` attribute when missing."""
    record = _BASE_FACTORY(*args, **kwargs)
    if not hasattr(record, "service"):
        record.service = _CURRENT_SERVICE or "unknown"
    return record


def configure_logging(service_name: str | None = None, level: str | None = None, *, force: bool = False) -> None:
    """Configure process-wide logging and (optionally) tag records with a service name.

    Args:
        service_name: Service identifier to attach to every LogRecord as `record.service`.
            If None, keeps the previously configured value.
        level: Log level name (e.g., "INFO"). If None, uses $LOG_LEVEL or "INFO".
        force: If True, reconfigure root handlers even if logging was already configured.
    """
    global _CURRENT_SERVICE
    if service_name:
        _CURRENT_SERVICE = service_name

    logging.setLogRecordFactory(_record_factory)

    level_name = (level or os.getenv("LOG_LEVEL", "INFO")).upper()
    level_value = logging.getLevelNamesMapping().get(level_name, logging.INFO)

    show_service = _CURRENT_SERVICE is not None
    fmt = (
        "%(asctime)s %(levelname)s %(name)s [service=%(service)s] - %(message)s"
        if show_service
        else "%(asctime)s %(levelname)s %(name)s - %(message)s"
    )

    logging.basicConfig(level=level_value, format=fmt, force=force)
