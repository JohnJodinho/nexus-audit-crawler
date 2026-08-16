import json
from pathlib import Path

_FILE_PATH = Path("app/output.jsonl")


def load_concatenated_json_objects(json_text: str) -> list[dict]:
    decoder = json.JSONDecoder()
    items: list[dict] = []
    index = 0
    text_length = len(json_text)

    while index < text_length:
        # Skip whitespace between objects.
        while index < text_length and json_text[index].isspace():
            index += 1
        if index >= text_length:
            break

        item, end = decoder.raw_decode(json_text, index)
        items.append(item)
        index = end

    return items


crawled_sites: list[dict] = []
with _FILE_PATH.open("r", encoding="utf-8") as f:
    json_text = f.read()
    crawled_sites = load_concatenated_json_objects(json_text)


print(f"Total sites crawled: {len(crawled_sites)}")

# print(crawled_sites[0].get("contacts", {}).get("emails", []))
# print(crawled_sites[0].get("contacts", {}).get("phones", []))

for site in crawled_sites:
    print(site.get("url"))
    for email in site.get("contacts", {}).get("emails", []):
        print(f"  Email: {email}")
    for phone in site.get("contacts", {}).get("phones", []):
        print(f"  Phone: {phone}")
