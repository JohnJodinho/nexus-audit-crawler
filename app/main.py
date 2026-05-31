"""
app/main.py
===========
Enterprise AI Audit Crawler – Ephemeral Execution Engine.

CRITICAL CONSTRAINT
-------------------
``Spider.start()`` is explicitly **NOT** used.  All execution goes through
``spider.stream()``, which is an async generator that yields items one-by-one
as they are scraped.  This enables real-time item writing without buffering
the entire result set in memory first.

Event loop strategy
-------------------
The documentation mentions ``uvloop`` (Linux/macOS) or ``winloop`` (Windows)
as optional faster event loops.  Neither is installed in this venv, so we use
``anyio`` with its default backend (``asyncio``).  The ``anyio.run()`` call
correctly handles the async entry point on all platforms.

If ``winloop`` or ``uvloop`` becomes available, swap the ``anyio.run()`` call
for::

    import winloop           # Windows
    winloop.run(main())

    import uvloop            # Linux / macOS
    uvloop.run(main())

Output format
-------------
Scraped items are written to ``./app/output.jsonl`` in JSON Lines format
(one JSON object per line, UTF-8 encoded).  This format is:
  - Streamable – items can be read/processed before the crawl finishes.
  - Append-safe – partial runs don't corrupt previously written data.
  - LLM-friendly – each line is a self-contained JSON document.

Target configuration
--------------------
Edit ``TARGET_URL`` and ``TARGET_DOMAIN`` below to point the spider at your
actual audit target before running.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# MUST be the very first executable statements – before any import that could
# trigger logging handler construction (including Scrapling's Spider base).
# On Windows, PowerShell/cmd default to the system codepage (e.g. cp1252).
# Reconfiguring stdout/stderr to UTF-8 here means every StreamHandler in the
# entire process – including the ones Scrapling creates internally for the
# spider logger – will use UTF-8, eliminating UnicodeEncodeError on non-ASCII chars.
# ---------------------------------------------------------------------------
import sys
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import json
import logging
import pathlib
import datetime
from typing import Any, Dict

import anyio

try:
    import winloop
except ImportError:
    winloop = None
# Import our spider (Waterfall Session Architecture)
from app.spider import AuditSpider

# Import the logger helpers so pipeline events land in the shared log file.
from app.logger import LOG_FILE_PATH, get_pipeline_logger


# ===========================================================================
# Configuration – edit these before running
# ===========================================================================

#: Seed URL – the starting point of the crawl.
TARGET_URL: str = "https://www.infinitylegal.com.sg"

#: Domain fence – only pages whose hostname matches this (or its subdomains)
#: will be crawled.  Must match the ``allowed_domains`` set on the spider.
TARGET_DOMAIN: str = "infinitylegal.com.sg"

#: Output file path – items are streamed here in JSON Lines format.
#: The directory is created automatically if it doesn't exist.
_APP_DIR = pathlib.Path(__file__).parent  # …/scrapling/app/
OUTPUT_PATH: pathlib.Path = _APP_DIR / "output.jsonl"


# ===========================================================================
# Pipeline logger
# ===========================================================================
# This logger is distinct from ``self.logger`` inside the Spider.
# It captures main-loop events (file I/O, startup, shutdown) and is
# routed to the same ``crawler.log`` file via the logger module helper.
log: logging.Logger = get_pipeline_logger("audit_crawler.main")


# ===========================================================================
# Async main loop
# ===========================================================================


async def main() -> None:
    """
    Async entry point – runs the AuditSpider via ``spider.stream()``.

    Execution sequence
    ------------------
    1. Configure the spider with the target URL and domain fence.
    2. Open the output JSONL file in append mode so partial runs survive.
    3. Iterate ``async for item in spider.stream()`` – this drives the
       Crawler Engine, which in turn manages sessions, concurrency, the
       scheduler, and blocked-request retries.
    4. Write each yielded item as a single UTF-8 JSON line to the file.
    5. Log real-time statistics after every item.
    6. On completion, log final crawl statistics.

    Error handling
    --------------
    ``KeyboardInterrupt`` is caught so that a ``Ctrl+C`` during the stream
    produces a clean log entry rather than an ugly traceback.  The Spider
    base class handles its own graceful shutdown internally.

    Any other exception propagating out of the stream is logged at CRITICAL
    level with a full traceback before the process exits.
    """
    log.info("=" * 70)
    log.info("Enterprise AI Audit Crawler – Phase 1 Execution Engine")
    log.info("=" * 70)
    log.info("Target URL:    %s", TARGET_URL)
    log.info("Target domain: %s", TARGET_DOMAIN)
    log.info("Output file:   %s", OUTPUT_PATH)
    log.info("Log file:      %s", LOG_FILE_PATH)
    log.info("=" * 70)

    # -------------------------------------------------------------------
    # Spider instantiation
    # -------------------------------------------------------------------
    # We dynamically override the class-level start_urls and allowed_domains
    # so this main.py can be configured without editing spider.py.
    AuditSpider.start_urls = [TARGET_URL]
    AuditSpider.allowed_domains = {TARGET_DOMAIN}

    spider = AuditSpider()

    # -------------------------------------------------------------------
    # Output file setup
    # -------------------------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Open in append mode – if the file already exists from a previous run,
    # new items are added after existing ones rather than overwriting.
    items_written: int = 0

    log.info("Opening output file (append mode): %s", OUTPUT_PATH)

    try:
        # anyio.open_file gives us an async file handle so we can write
        # without blocking the event loop on disk I/O.
        async with await anyio.open_file(
            OUTPUT_PATH, mode="a", encoding="utf-8"
        ) as out_file:
            log.info("Spider stream starting…")

            # ---------------------------------------------------------------
            # CRITICAL: Use spider.stream(), NOT spider.start()
            # ---------------------------------------------------------------
            # ``stream()`` is an async generator.  Each yielded value is a
            # validated, non-empty item dict (after on_scraped_item() ran).
            # The generator drives the entire Crawler Engine internally.
            async for item in spider.stream():
                item_with_meta: Dict[str, Any] = _enrich_item(
                    item, items_written)

                # Serialise to a single JSON line and write immediately.
                # ``ensure_ascii=False`` preserves international characters.
                json_line: str = json.dumps(item_with_meta, ensure_ascii=False)
                await out_file.write(json_line + "\n")

                # Flush after every write so the file is never stale on disk.
                await out_file.flush()

                items_written += 1

                # Real-time progress log.
                _log_progress(spider, item_with_meta, items_written)

    except KeyboardInterrupt:
        # The user pressed Ctrl+C.  The spider handles graceful shutdown
        # internally via its signal handler.  We just log and exit cleanly.
        log.warning("KeyboardInterrupt received – crawl interrupted.")

    except Exception as exc:  # pragma: no cover
        log.critical(
            "Unhandled exception in main execution loop: %s",
            exc,
            exc_info=True,
        )
        raise

    finally:
        # Final statistics summary – available even if the crawl was interrupted.
        _log_final_stats(spider, items_written)


# ===========================================================================
# Helper functions
# ===========================================================================


def _enrich_item(item: Dict[str, Any], index: int) -> Dict[str, Any]:
    """
    Add pipeline-level metadata to a scraped item before writing.

    We do not modify the canonical keys (``url``, ``extracted_json_state``,
    ``extracted_markdown``) – those are the contract.  Instead we inject
    metadata under an ``_meta`` key so the data fields remain unambiguous.

    Parameters
    ----------
    item:
        The raw item dict from the spider.
    index:
        Zero-based item index in the current run (for ordering).

    Returns
    -------
    dict
        Item with ``_meta`` block appended.
    """
    import datetime

    enriched = dict(item)
    enriched["_meta"] = {
        "item_index": index,
        "scraped_at_utc": datetime.datetime.now(datetime.UTC).isoformat(),
        "spider_name": "audit_spider",
    }
    return enriched


def _log_progress(spider: AuditSpider, item: Dict[str, Any], count: int) -> None:
    """
    Emit a real-time progress log entry after each item is written.

    Parameters
    ----------
    spider:
        The running spider – used to read live stats.
    item:
        The item that was just written.
    count:
        Number of items written so far (1-based at call time).
    """
    url = item.get("url", "<unknown>")
    has_json = bool(item.get("extracted_json_state"))
    has_md = bool(item.get("extracted_markdown", "").strip())
    extraction_type = "JSON" if has_json else (
        "Markdown" if has_md else "EMPTY")

    stats = getattr(spider, "stats", None)
    requests_count = getattr(stats, "requests_count", "?")
    blocked_count = getattr(stats, "blocked_requests_count", "?")

    log.info(
        "[ITEM #%d] %s | Type: %s | Total requests: %s | Blocked: %s",
        count,
        url,
        extraction_type,
        requests_count,
        blocked_count,
    )


def _log_final_stats(spider: AuditSpider, items_written: int) -> None:
    """
    Emit a final summary of crawl statistics when the run completes.

    Parameters
    ----------
    spider:
        The spider instance (may have exited stream already).
    items_written:
        Total items successfully written to the output file.
    """
    # spider.stats is a property that raises RuntimeError (not AttributeError)
    # when accessed after the stream has ended.  getattr's default only catches
    # AttributeError, so we must use an explicit try/except here.
    try:
        stats = spider.stats
    except RuntimeError:
        stats = None
    log.info("=" * 70)
    log.info("CRAWL COMPLETE")
    log.info("Items written to %s: %d", OUTPUT_PATH, items_written)
    if stats:
        log.info(
            "Total requests made:          %s", getattr(
                stats, "requests_count", "?")
        )
        log.info(
            "Failed requests:              %s",
            getattr(stats, "failed_requests_count", "?"),
        )
        log.info(
            "Blocked requests (total):     %s",
            getattr(stats, "blocked_requests_count", "?"),
        )
        log.info(
            "Offsite requests filtered:    %s",
            getattr(stats, "offsite_requests_count", "?"),
        )
        log.info(
            "Items scraped (spider):       %s", getattr(
                stats, "items_scraped", "?")
        )
        log.info(
            "Items dropped (spider):       %s", getattr(
                stats, "items_dropped", "?")
        )
        log.info(
            "Response bytes received:      %s", getattr(
                stats, "response_bytes", "?")
        )
        elapsed = getattr(stats, "elapsed_seconds", None)
        if elapsed is not None:
            log.info("Elapsed time:                 %.1f seconds", elapsed)
        rps = getattr(stats, "requests_per_second", None)
        if rps is not None:
            log.info("Throughput:                   %.2f req/s", rps)
    log.info("=" * 70)


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    """
    Run the audit crawler from the command line:

        cd scrapling
        .\\venv\\Scripts\\python.exe -m app.main

    Or from outside the scrapling directory:

        python -m app.main

    ``anyio.run()`` selects the asyncio backend by default.  On Windows,
    replace with ``winloop.run(main())`` once winloop is installed for a
    performance boost.
    """
    anyio.run(main)
