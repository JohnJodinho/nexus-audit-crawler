from trafilatura import deduplication
import json
from collections import defaultdict


def find_depulicate_urls(data_list):
    seen_urls = set()

    duplicate_objects = []

    duplicate_counts = defaultdict(int)

    for entry in data_list:
        url = entry.get("url")

        if not url:
            continue

        if url in seen_urls:
            duplicate_objects.append(entry)
            duplicate_counts[url] += 1
        else:
            seen_urls.add(url)

    return {
        "seen_urls": list(seen_urls),
        "total_duplicates": len(duplicate_objects),
        "duplicate_counts": dict(duplicate_counts),
        "duplicate_objects": duplicate_objects,
    }


if __name__ == "__main__":
    data_path = "audit_results.json"
    data = []
    dup_file = "audit_results_dups.json"
    with open(data_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    print(f"total items: {len(data)}")
    results = find_depulicate_urls(data)
    if results["total_duplicates"] > 0:
        with open(dup_file, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=4)
    else:
        print(f"Found no duplicates in {data_path}")
