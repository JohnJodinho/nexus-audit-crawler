"""Compare a baseline fetch with bounded browser interaction extraction.

Usage:
    python scripts/dynamic_site_extraction.py

The script uses the project's Playwright dependency directly and saves
artifacts under ``misc/dynamic-extraction/`` for manual comparison.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

from playwright.async_api import async_playwright

URL = "https://4bruvver.base44.app/about"
OUTPUT = Path("misc/dynamic-extraction")


async def extract(page, *, explore: bool) -> dict[str, object]:
    await page.goto(URL, wait_until="domcontentloaded", timeout=60_000)
    if explore:
        await page.evaluate(
            """async () => {
                const sleep = ms => new Promise(r => setTimeout(r, ms));
                const seen = new Set();
                for (let step = 0; step < 12; step += 1) {
                    window.scrollTo(0, Math.min(document.body.scrollHeight,
                        window.scrollY + Math.max(window.innerHeight * 0.8, 400)));
                    await sleep(500);
                    for (const element of [...document.querySelectorAll(
                        'button, summary, [aria-expanded="false"]'
                    )].slice(0, 40)) {
                        const text = (element.innerText || element.getAttribute('aria-label') || '').trim();
                        const type = (element.getAttribute('type') || '').toLowerCase();
                        const reveal = /load|more|show|expand|read|tab|accordion|next|previous/i.test(text);
                        if (!text || element.closest('a') || ['submit', 'file', 'reset'].includes(type)
                            || !(element.tagName.toLowerCase() === 'summary'
                                || element.getAttribute('aria-expanded') === 'false' || reveal)
                            || /^(login|sign in|log in|subscribe|buy|checkout|delete|logout)$/i.test(text)) continue;
                        const key = `${text}|${element.tagName}`;
                        if (seen.has(key)) continue;
                        seen.add(key);
                        try { element.click(); } catch (_) {}
                        await sleep(500);
                    }
                    if (window.scrollY + window.innerHeight >= document.body.scrollHeight - 4) break;
                }
                window.scrollTo(0, 0);
                await sleep(500);
            }"""
        )
    text = await page.locator("body").inner_text()
    html = await page.content()
    return {
        "url": page.url,
        "title": await page.title(),
        "text": text,
        "text_length": len(text),
        "html_length": len(html),
        "headings": await page.locator("h1, h2, h3").all_inner_texts(),
        "links": await page.locator("a").all_inner_texts(),
        "terms": sorted(set(re.findall(r"\b[A-Za-z][A-Za-z'-]{3,}\b", text.lower()))),
        "html": html,
    }


async def main() -> None:
    OUTPUT.mkdir(parents=True, exist_ok=True)
    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(headless=True)
        baseline = await extract(await browser.new_page(), explore=False)
        improved = await extract(await browser.new_page(), explore=True)
        await browser.close()

    for name, result in (("baseline", baseline), ("improved", improved)):
        (OUTPUT / f"{name}.html").write_text(str(result["html"]), encoding="utf-8")
        (OUTPUT / f"{name}.json").write_text(
            json.dumps({key: value for key, value in result.items() if key != "html"}, indent=2),
            encoding="utf-8",
        )
    added_terms = sorted(set(improved["terms"]) - set(baseline["terms"]))
    comparison = {
        "url": URL,
        "baseline_text_length": baseline["text_length"],
        "improved_text_length": improved["text_length"],
        "added_terms": added_terms,
        "baseline_headings": baseline["headings"],
        "improved_headings": improved["headings"],
    }
    (OUTPUT / "comparison.json").write_text(json.dumps(comparison, indent=2), encoding="utf-8")
    print(json.dumps(comparison, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
