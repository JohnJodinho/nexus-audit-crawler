"""
app/logger.py
=============
Centralised logging configuration for the Enterprise AI Audit Crawler.

Provides:
- ``LOG_FILE_PATH``: canonical log path used by the Spider's ``log_file`` attribute.
- ``attach_file_handler()``: wires a FileHandler onto any logger.
- ``get_pipeline_logger()``: multi-sink factory that routes logs to three
  distinct files by component prefix.

Log sinks
---------
crawler_errors.log  WARNING+  no filter     all components
worker_system.log   INFO+     audit_crawler worker-loop events only
spider_activity.log INFO+     audit_spider  spider-level events only
"""

import logging
import pathlib

_APP_DIR = pathlib.Path(__file__).parent
_LOG_DIR = _APP_DIR / "logs"

LOG_FILE_PATH: str = str(_LOG_DIR / "crawler.log")

LOG_FORMAT: str = "[%(asctime)s]:(%(name)s) %(levelname)s: %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"


class ComponentFilter(logging.Filter):
    """Allow only records whose ``record.name`` starts with ``prefix``."""

    def __init__(self, prefix: str) -> None:
        super().__init__()
        self._prefix = prefix

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name.startswith(self._prefix)


def _make_file_handler(
    filename: str,
    level: int,
    component_filter: "ComponentFilter | None" = None,
) -> logging.FileHandler:
    """Create a FileHandler for ``_LOG_DIR/filename`` at ``level``, with an optional filter."""
    log_path = _LOG_DIR / filename
    log_path.parent.mkdir(parents=True, exist_ok=True)
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(level)
    handler.setFormatter(formatter)
    if component_filter is not None:
        handler.addFilter(component_filter)
    return handler


def attach_file_handler(
    logger: logging.Logger,
    level: int = logging.DEBUG,
) -> logging.FileHandler:
    """
    Attach a FileHandler writing to ``LOG_FILE_PATH`` to ``logger``.

    Idempotent: skips attachment if an equivalent handler already exists.

    Parameters
    ----------
    logger:
        Target logger.
    level:
        Minimum severity level for the handler.

    Returns
    -------
    logging.FileHandler
        The attached (or existing) handler.
    """
    log_path = pathlib.Path(LOG_FILE_PATH)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    for handler in logger.handlers:
        if isinstance(handler, logging.FileHandler):
            if pathlib.Path(handler.baseFilename).resolve() == log_path.resolve():
                return handler

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return file_handler


def get_pipeline_logger(name: str = "audit_crawler.pipeline") -> logging.Logger:
    """
    Return a named logger wired to three file sinks and a console handler.

    Also wires the ``spider_activity.log`` sink directly to the
    ``audit_spider`` logger so that records emitted by ``self.logger``
    inside ``AuditSpider`` are captured without relying on propagation.

    Parameters
    ----------
    name:
        Logger name, e.g. ``"audit_crawler.main"``.

    Returns
    -------
    logging.Logger
        Configured logger; idempotent on repeated calls.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    logger.addHandler(
        _make_file_handler("crawler_errors.log", level=logging.WARNING)
    )
    logger.addHandler(
        _make_file_handler(
            "worker_system.log",
            level=logging.INFO,
            component_filter=ComponentFilter("audit_crawler"),
        )
    )

    spider_activity_handler = _make_file_handler(
        "spider_activity.log",
        level=logging.INFO,
        component_filter=ComponentFilter("audit_spider"),
    )
    logger.addHandler(spider_activity_handler)

    # Fix: also attach spider_activity.log directly to the spider's own logger.
    # self.logger inside AuditSpider is a separate Logger instance named
    # "audit_spider" (derived from spider.name).  Records it emits never flow
    # through this logger, so the ComponentFilter above never sees them.
    # Attaching the handler directly ensures spider events are captured.
    spider_logger = logging.getLogger("audit_spider")
    spider_logger.setLevel(logging.DEBUG)
    if not any(
        isinstance(h, logging.FileHandler)
        and pathlib.Path(h.baseFilename).name == "spider_activity.log"
        for h in spider_logger.handlers
    ):
        spider_logger.addHandler(
            _make_file_handler("spider_activity.log", level=logging.INFO)
        )

    logger.propagate = False

    return logger
