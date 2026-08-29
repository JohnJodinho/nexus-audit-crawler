import json
from pathlib import Path

_FILE_PATH = Path("setia_results.json")


def load_items(path: Path) -> list[dict]:
    if not path.exists():
        print(f"File {path} does not exist.")
        return []
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


if __name__ == "__main__":
    crawled_sites = load_items(_FILE_PATH)
    print(f"Total sites crawled: {len(crawled_sites)}")

    for site in crawled_sites:
        print(f"URL: {site.get('url')}")
        contacts = site.get("contacts", {})
        if isinstance(contacts, str):
            try:
                contacts = json.loads(contacts)
            except Exception:
                contacts = {}
        for email in contacts.get("emails", []):
            print(f"  Email: {email}")
        for phone in contacts.get("phones", []):
            print(f"  Phone: {phone}")
