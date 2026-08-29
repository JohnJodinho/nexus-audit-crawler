import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict, List

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.config import settings
from app.redis_client import (
    create_redis_pool,
    dlq_key,
    results_key,
    tasks_key,
    telemetry_key,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("data_exporter")


def get_stream_key(stream_alias: str, crawl_id: str) -> str:
    mapping = {
        "results": results_key(crawl_id),
        "telemetry": telemetry_key(crawl_id),
        "dlq": dlq_key(crawl_id),
        "tasks": tasks_key(crawl_id),
    }
    return mapping.get(stream_alias, "")


async def export_stream(
    stream_alias: str,
    output_file: str,
    crawl_id: str,
    batch_size: int,
    clear: bool,
):
    """Reads all messages from a specified stream and exports them to JSON."""
    redis = create_redis_pool()
    stream_key = get_stream_key(stream_alias, crawl_id)

    if not stream_key:
        log.error(
            "Unknown stream '%s'. Choose from: results, telemetry, dlq, tasks",
            stream_alias,
        )
        return

    log.info("Connecting to Redis... Target stream: %s", stream_key)

    exported_data: List[Dict[str, Any]] = []
    message_ids_to_delete: List[str] = []

    cursor = "-"

    try:
        while True:
            batch = await redis.xrange(
                stream_key, min=cursor, max="+", count=batch_size
            )

            if not batch:
                break

            for message_id, data in batch:
                data["_message_id"] = message_id
                # Deserialize any JSON strings in the payload for cleaner exported JSON
                for k, v in list(data.items()):
                    if isinstance(v, str) and (v.startswith("{") or v.startswith("[")):
                        try:
                            data[k] = json.loads(v)
                        except Exception:
                            pass
                exported_data.append(data)
                message_ids_to_delete.append(message_id)

            last_id = batch[-1][0]
            time_part, seq_part = last_id.split("-")
            cursor = f"{time_part}-{int(seq_part) + 1}"

        log.info("Extracted %d records from %s.", len(exported_data), stream_key)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(exported_data, f, indent=2, ensure_ascii=False)
        log.info("Successfully wrote data to %s", output_file)

        if clear and message_ids_to_delete:
            log.warning(
                "Clearing %d messages from %s...",
                len(message_ids_to_delete),
                stream_key,
            )
            chunk_size = 1000
            for i in range(0, len(message_ids_to_delete), chunk_size):
                chunk = message_ids_to_delete[i : i + chunk_size]
                await redis.xdel(stream_key, *chunk)
            log.info("Stream %s cleared.", stream_key)

    except Exception as e:
        log.error("Error during export: %s", e)
    finally:
        await redis.aclose()


def main():
    parser = argparse.ArgumentParser(description="Export Redis Stream data to JSON.")

    parser.add_argument(
        "stream",
        choices=["results", "telemetry", "dlq", "tasks"],
        help="Which stream to export (results, telemetry, dlq, tasks)",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="export.json",
        help="Output JSON file path (default: export.json)",
    )

    parser.add_argument(
        "--crawl-id",
        default=settings.CRAWL_ID,
        help=f"Crawl ID to export from (default: {settings.CRAWL_ID})",
    )

    parser.add_argument(
        "--clear",
        action="store_true",
        help="Delete the messages from the Redis stream after exporting",
    )

    args = parser.parse_args()

    asyncio.run(
        export_stream(
            stream_alias=args.stream,
            output_file=args.output,
            crawl_id=args.crawl_id,
            batch_size=1000,
            clear=args.clear,
        )
    )


if __name__ == "__main__":
    main()
