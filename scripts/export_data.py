import argparse
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
import json
import logging
from typing import List, Dict, Any

# Import your existing connection pool
from app.redis_client import create_redis_pool

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("data_exporter")

# Map friendly CLI names to actual Redis stream keys
STREAM_MAP = {
    "results": "stream:audit_results",
    "telemetry": "stream:dropped_telemetry",
    "dlq": "stream:dlq",
    "tasks": "stream:audit_tasks",
}


async def export_stream(
    stream_alias: str, output_file: str, batch_size: int, clear: bool
):
    """Reads all messages from a specified stream and exports them to JSON."""
    redis = create_redis_pool()
    stream_key = STREAM_MAP.get(stream_alias)

    if not stream_key:
        log.error(
            f"Unknown stream '{stream_alias}'. Choose from: {list(STREAM_MAP.keys())}"
        )
        return

    log.info(f"Connecting to Redis... Target stream: {stream_key}")

    exported_data: List[Dict[str, Any]] = []
    message_ids_to_delete: List[str] = []

    # Start at the very beginning of the stream
    cursor = "-"

    try:
        while True:
            # XRANGE <stream> <start> <end> COUNT <n>
            # The '+' means the end of the stream.
            batch = await redis.xrange(
                stream_key, min=cursor, max="+", count=batch_size
            )

            if not batch:
                break

            for message_id, data in batch:
                # Add the Redis timestamp/ID to the payload for traceability
                data["_message_id"] = message_id
                exported_data.append(data)
                message_ids_to_delete.append(message_id)

            # Update the cursor to the last seen ID + 1 to get the next batch
            last_id = batch[-1][0]
            # In Redis, appending '-0' to a timestamp (or incrementing the sequence) moves it forward
            # An easy trick is to just use the last_id as the next start, but we must slice the first element
            # of the next batch to avoid duplicating the boundary message.
            # To avoid the slice logic, we can construct the exclusive ID:
            time_part, seq_part = last_id.split("-")
            cursor = f"{time_part}-{int(seq_part) + 1}"

        log.info(f"Extracted {len(exported_data)} records from {stream_key}.")

        # Write to JSON file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(exported_data, f, indent=4, ensure_ascii=False)
        log.info(f"✅ Successfully wrote data to {output_file}")

        # Optional: Clean up the stream if the user requested it
        if clear and message_ids_to_delete:
            log.warning(
                f"Clearing {len(message_ids_to_delete)} messages from {stream_key}..."
            )
            # XDEL <stream> <id1> <id2> ...
            # We chunk the deletes so we don't exceed Redis command length limits
            chunk_size = 1000
            for i in range(0, len(message_ids_to_delete), chunk_size):
                chunk = message_ids_to_delete[i : i + chunk_size]
                await redis.xdel(stream_key, *chunk)
            log.info(f"🧹 Stream {stream_key} cleared.")

    except Exception as e:
        log.error(f"Error during export: {e}")
    finally:
        await redis.aclose()
        # Give time for the connection pool to close gracefully
        del redis

        import gc

        gc.collect()  # Force garbage collection to clean up any lingering connections

        # Small delay to ensure all async cleanup is done before exiting
        await asyncio.sleep(0.1)


def main():

    # if sys.platform == "win32":
    #     # On Windows, the default event loop does not support subprocesses well.
    #     # This is a workaround to ensure compatibility.
    #     asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    parser = argparse.ArgumentParser(description="Export Redis Stream data to JSON.")

    parser.add_argument(
        "stream",
        choices=list(STREAM_MAP.keys()),
        help="Which stream to export (results, telemetry, tasks or dlq)",
    )

    parser.add_argument(
        "-o",
        "--output",
        default="export.json",
        help="Output JSON file path (default: export.json)",
    )

    parser.add_argument(
        "--clear",
        action="store_true",
        help="Delete the messages from the Redis stream after exporting",
    )

    args = parser.parse_args()

    # Run the async export
    asyncio.run(
        export_stream(
            stream_alias=args.stream,
            output_file=args.output,
            batch_size=1000,
            clear=args.clear,
        )
    )


if __name__ == "__main__":
    main()
