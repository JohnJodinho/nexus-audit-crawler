"""
app/telemetry.py
================
Structured Telemetry Sink for the Enterprise AI Audit Crawler.

Architectural pattern: "Fire-and-forget async sink"
----------------------------------------------------
The spider's ``parse()`` callback is an async generator running on the
asyncio event loop.  Any I/O it performs must be non-blocking, otherwise
it stalls the entire crawler engine.

The ``TelemetrySink`` class uses ``anyio.open_file()`` to open the output
file as an async stream, then exposes a single ``record()`` coroutine that
serialises one drop-event to a JSON Line and flushes it immediately.  The
caller simply ``await``s the record – no threads, no queues, no blocking.

Output schema
-------------
Every line written to ``dropped_telemetry.jsonl`` is a JSON object with
exactly these four keys::

    {
        "timestamp_utc": "2026-05-30T13:00:00.000000+00:00",  # ISO-8601 w/ tz
        "source_url":    "https://example.com/about",          # page being parsed
        "target_url":    "https://example.com/login",          # link that was dropped
        "drop_reason":   "BOUNDARY_EXCLUSION"                  # see DropReason below
    }

Drop reasons
------------
``INVALID_SCHEME``
    The href uses a non-HTTP/S scheme (``mailto:``, ``tel:``, ``javascript:``,
    etc.) that cannot be crawled.  Note: ``mailto:`` and ``tel:`` links are
    NOT recorded with this reason -- they produce the more specific
    ``CONTACT_EXTRACTED_EMAIL`` / ``CONTACT_EXTRACTED_PHONE`` reasons below.

``DENY_LIST``
    The URL path matched ``DENY_PATTERN`` -- it is an explicitly suppressed
    low-value page (blog, news, login, careers, events, etc.).

``MAX_DEPTH_REACHED``
    The crawler has reached ``max_depth`` from the seed URL.  No further
    links are extracted from pages at this depth, preventing unbounded
    traversal now that the allow whitelist has been removed.

``DUPLICATE``
    The URL passed all scope filters but had already been queued for fetching
    in a previous iteration of ``parse()``.  Emitting this event proves that
    the pre-scheduler deduplication gate is active.

``CONTACT_EXTRACTED_EMAIL``
    The href was a ``mailto:`` link.  The email address was extracted, cleaned,
    and added to the page payload's ``contacts.emails`` list before the link
    was dropped from the traversal queue.  Target URL in the event is the
    raw ``mailto:`` href (pre-cleaning) for full auditability.

``CONTACT_EXTRACTED_PHONE``
    The href was a ``tel:`` link.  The phone number was extracted, cleaned,
    and added to the page payload's ``contacts.phones`` list before the link
    was dropped from the traversal queue.  Target URL is the raw ``tel:``
    href.
"""

from __future__ import annotations

import datetime
import json
import pathlib
from enum import Enum
from typing import Optional

import anyio
import anyio.abc


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_APP_DIR: pathlib.Path = pathlib.Path(__file__).parent
TELEMETRY_PATH: pathlib.Path = _APP_DIR / "logs" / "dropped_telemetry.jsonl"


# ---------------------------------------------------------------------------
# Drop reason enum
# ---------------------------------------------------------------------------

class DropReason(str, Enum):
    """
    Canonical vocabulary for link drop events written to the telemetry sink.

    Inheriting from ``str`` means the enum value serialises as a plain string
    in ``json.dumps()`` without needing a custom encoder -- keeping the sink
    code simple and the output human-readable.
    """
    INVALID_SCHEME          = "INVALID_SCHEME"
    DENY_LIST               = "DENY_LIST"               # path matched DENY_PATTERN
    MAX_DEPTH_REACHED       = "MAX_DEPTH_REACHED"       # crawl depth limit hit
    DUPLICATE               = "DUPLICATE"               # URL already queued this session
    CONTACT_EXTRACTED_EMAIL = "CONTACT_EXTRACTED_EMAIL" # mailto: -- email harvested
    CONTACT_EXTRACTED_PHONE = "CONTACT_EXTRACTED_PHONE" # tel:    -- phone harvested


# ---------------------------------------------------------------------------
# Telemetry sink
# ---------------------------------------------------------------------------

class TelemetrySink:
    """
    Async JSONL writer for link drop-event telemetry.

    Lifecycle
    ---------
    The sink must be opened before the crawl begins and closed when it ends::

        sink = TelemetrySink()
        await sink.open()          # opens the file handle
        ...
        await sink.record(...)     # called from parse() for each dropped link
        ...
        await sink.close()         # flushes and closes the file handle

    The ``open()`` / ``close()`` calls map directly to ``on_start()`` and
    ``on_close()`` in the spider, keeping resource lifetimes aligned with the
    crawl session.

    Error handling
    --------------
    ``record()`` wraps every write in a ``try/except`` so that a transient I/O
    error (e.g. disk full) is logged but does not propagate into the spider and
    crash the crawl.  Telemetry is observability infrastructure – it must never
    be on the critical path.

    Parameters
    ----------
    path:
        Destination JSONL file.  Defaults to ``app/logs/dropped_telemetry.jsonl``.
    """

    def __init__(self, path: pathlib.Path = TELEMETRY_PATH) -> None:
        self._path: pathlib.Path = path
        # Async file handle – populated by open(), cleared by close().
        self._file: Optional[anyio.abc.AsyncResource] = None

    async def open(self) -> None:
        """
        Open the telemetry file in async append mode.

        Append mode (``"a"``) means:
        - Existing events from previous partial runs are preserved.
        - Multiple processes writing to the same file won't corrupt it
          (each ``write()`` is a single atomic syscall on POSIX; good
          enough for our single-process use case).

        The parent directory is created automatically so there is no
        dependency on the logs/ directory existing beforehand.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._file = await anyio.open_file(
            self._path, mode="a", encoding="utf-8"
        )

    async def record(
        self,
        source_url: str,
        target_url: str,
        drop_reason: DropReason,
    ) -> None:
        """
        Serialise and append a single drop-event to the telemetry file.

        This coroutine is designed to be ``await``ed directly from ``parse()``
        without any additional orchestration.  Each call produces exactly one
        UTF-8 JSON Line terminated by ``\\n`` and immediately flushes it to
        disk so the file is never stale mid-crawl.

        Parameters
        ----------
        source_url:
            The URL of the page currently being parsed (where the link was
            found).
        target_url:
            The absolute URL of the link that was dropped.
        drop_reason:
            One of the ``DropReason`` enum members describing why it was dropped.
        """
        if self._file is None:
            # Defensive guard – record() called before open() or after close().
            # Silent no-op: telemetry must never crash the spider.
            return

        event: dict = {
            "timestamp_utc": datetime.datetime.now(datetime.UTC).isoformat(),
            "source_url":    source_url,
            "target_url":    target_url,
            "drop_reason":   drop_reason.value,   # str(enum) via str mixin
        }

        try:
            line: str = json.dumps(event, ensure_ascii=False)
            await self._file.write(line + "\n")
            await self._file.flush()
        except Exception:
            # Swallow I/O errors silently – telemetry loss is acceptable,
            # crawl interruption is not.  The spider logger will surface any
            # systemic issue via its own error handlers.
            pass

    async def close(self) -> None:
        """
        Flush and close the underlying async file handle.

        Safe to call even if ``open()`` was never called (e.g. if the spider
        was force-killed during session setup before ``on_start`` completed).
        """
        if self._file is not None:
            try:
                await self._file.aclose()
            except Exception:
                pass
            finally:
                self._file = None
