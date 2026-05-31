from trafilatura import fetch_url, extract


def download_webdata(url: str):
    downloaded = fetch_url(url)
    if downloaded is None:
        print(f"Failed to download {url}")
        return None
    return downloaded


def extract_and_save_webdata(html):
    extracted = extract(html, output_format="markdown")
    if extracted is None:
        print("Failed to extract data")
        return None
    return extracted


def main():
    urls = [
        # "https://scrapling.readthedocs.io/en/latest/spiders/architecture.html",
        # "https://scrapling.readthedocs.io/en/latest/spiders/getting-started.html",
        # "https://scrapling.readthedocs.io/en/latest/spiders/requests-responses.html",
        # "https://scrapling.readthedocs.io/en/latest/spiders/sessions.html",
        # "https://scrapling.readthedocs.io/en/latest/spiders/proxy-blocking.html",
        # "https://scrapling.readthedocs.io/en/latest/spiders/generic-templates.html",
        # "https://scrapling.readthedocs.io/en/latest/spiders/advanced.html"
        "https://scrapling.readthedocs.io/en/latest/fetching/stealthy.html"
    ]

    for url in urls:
        print(f"Downloading {url}...")
        html = download_webdata(url)
        if html is not None:
            print(f"Extracting data from {url}...")
            extracted = extract_and_save_webdata(html)
            if extracted is not None:
                print(f"Successfully extracted data from {url}")
                # print(f"Extracted data from {url}:\n{extracted[:500]}...\n")
                with open(f"{url.rsplit('/', maxsplit=1)[-1].split('.')[0].replace('-', '_')}.md", "w", encoding="utf-8") as f:
                    f.write(extracted)
            else:
                print(f"Failed to extract data from {url}")
        else:
            print(f"Failed to download {url}")


if __name__ == "__main__":

    main()
