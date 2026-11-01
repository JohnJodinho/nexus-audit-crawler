"""Command-line entrypoint for applying database migrations."""

from __future__ import annotations

import asyncio
import logging

from app.db.migrations import run_migrations


def main() -> None:
    """Apply all pending migrations and exit non-zero on failure."""
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run_migrations())


if __name__ == "__main__":
    main()
