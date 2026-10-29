import io
from markitdown import MarkItDown
from playwright.sync_api import sync_playwright


def bypass_and_convert(url: str):
    print(f"[*] Initializing stealth browser for: {url}")

    with sync_playwright() as p:
        # Launch Chromium with anti-detection configuration arguments
        browser = p.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--window-size=1920,1080",
            ],
        )

        # Set up a realistic browser context with headers
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,"
                " like Gecko) Chrome/122.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
        )

        page = context.new_page()

        # Mask the navigator.webdriver property to evade basic bot detection scripts
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
        """)

        html_content = ""
        try:
            print("[*] Navigating to target website...")
            page.goto(url, timeout=60000, wait_until="domcontentloaded")

            # Give extra breathing room for dynamic elements or scripts to render
            page.wait_for_timeout(4000)

            # Extract the fully hydrated DOM content
            html_content = page.content()
            print("[+] Successfully bypassed blockers and retrieved page content.")

        except Exception as e:
            print(f"[-] Error loading page: {e}")
        finally:
            browser.close()

    if not html_content:
        print("[-] No content to convert.")
        return

    print("[*] Passing content to Microsoft's MarkItDown...")

    # Initialize Microsoft's MarkItDown converter
    md_parser = MarkItDown()

    # Stream the HTML bytes directly into MarkItDown with the correct file extension
    stream = io.BytesIO(html_content.encode("utf-8"))
    result = md_parser.convert_stream(stream, file_extension=".html")

    # Output options
    markdown_output = result.text_content

    print("\n--- Converted Markdown Output Preview ---\n")
    print(
        markdown_output[:1000]
        + ("\n... [truncated]" if len(markdown_output) > 1000 else "")
    )

    # Save the result to a Markdown file
    output_filename = "scraped_content.md"
    with open(output_filename, "w", encoding="utf-8") as f:
        f.write(markdown_output)
    print(f"\n[+] Full Markdown successfully saved to '{output_filename}'")


if __name__ == "__main__":
    # Replace with the URL you want to scrape
    target_url = "https://zibeh.ng/"
    bypass_and_convert(target_url)
