"""
app/logger.py
=============
Centralised logging configuration for the Enterprise AI Audit Crawler.

Architecture note
-----------------
Scrapling v0.4 spiders manage their own ``self.logger`` instance.  The spider
will automatically create the log directory and attach a ``FileHandler`` when
the ``log_file`` class attribute is set on the Spider subclass.  This module
therefore has two responsibilities:

1.  Expose the **canonical log-file path** as a single source-of-truth
    constant (``LOG_FILE_PATH``) so that spider.py and main.py both agree on
    where logs land.

2.  Provide ``attach_file_handler()`` – a helper that wires a plain
    ``logging.FileHandler`` onto *any* standard Python logger (e.g. the
    ``main.py`` root logger) so that top-level asyncio errors and pipeline
    events are also captured in the same file.
"""

import logging
import pathlib

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# All log output – spider events, block events, pipeline errors – is funnelled
# into this single file.  Using a path relative to this module's parent keeps
# the project self-contained and avoids hardcoded absolute paths.
_APP_DIR = pathlib.Path(__file__).parent        # …/scrapling/app/
LOG_FILE_PATH: str = str(_APP_DIR / "logs" / "crawler.log")

# Human-readable format that matches the spider's default template,
# but also prefixes the logger name so entries from main.py are distinct.
LOG_FORMAT: str = "[%(asctime)s]:(%(name)s) %(levelname)s: %(message)s"
LOG_DATE_FORMAT: str = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def attach_file_handler(
    logger: logging.Logger,
    level: int = logging.DEBUG,
) -> logging.FileHandler:
    """
    Attach a ``FileHandler`` to *logger* that writes to ``LOG_FILE_PATH``.

    The parent directories are created automatically (mirrors what the Spider
    class does internally).  Calling this on the same logger multiple times is
    safe – the function is idempotent; it skips adding a new handler if an
    equivalent ``FileHandler`` is already attached.

    Parameters
    ----------
    logger:
        Any ``logging.Logger`` instance – typically the ``__name__``-based
        logger created in ``main.py``.
    level:
        Minimum severity to record to file.  Defaults to ``DEBUG`` so nothing
        is silently swallowed.

    Returns
    -------
    logging.FileHandler
        The handler that was attached (or found already attached).
    """
    log_path = pathlib.Path(LOG_FILE_PATH)
    log_path.parent.mkdir(parents=True, exist_ok=True)

    # Idempotency guard – don't attach a second FileHandler for the same path.
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
    Return a named logger configured for pipeline-level events.

    This logger writes to both the console (INFO and above) and the shared
    log file (DEBUG and above).  Use it in ``main.py`` for all asyncio loop
    and file I/O events that happen *outside* the Spider class.

    Parameters
    ----------
    name:
        Logger name.  Using a dotted-namespace keeps it distinct from the
        Scrapling internal loggers (``scrapling.spiders.*``).
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid duplicate handlers if this is called more than once.
    if logger.handlers:
        return logger

    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)

    # Console handler – INFO+ only, keeps the terminal readable.
    # stdout/stderr are reconfigured to UTF-8 in main.py before any logger
    # is constructed, so this plain StreamHandler is safe for Unicode output.
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler – DEBUG+, full verbosity in the log file.
    attach_file_handler(logger, level=logging.DEBUG)

    # Do not propagate to the root logger to prevent double-printing.
    logger.propagate = False

    return logger
