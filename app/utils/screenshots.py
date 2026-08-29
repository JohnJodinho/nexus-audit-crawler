"""
app/utils/screenshots.py
========================
Advanced full-page screenshot engine with PIL vertical stitching.

Solves the virtual DOM unmounting problem (e.g. react-virtualized, react-window)
where offscreen elements disappear during a single full-page resize, and eliminates
duplicate sticky headers by suppressing position:fixed elements on subsequent slices.
"""

from __future__ import annotations

import asyncio
import io
import logging
from typing import Any, Optional

log = logging.getLogger("audit_crawler.screenshots")


async def capture_stitched_screenshot(
    page: Any,
    max_slices: int = 10,
    slice_delay_ms: int = 150,
) -> Optional[bytes]:
    """
    Capture a stitched full-page screenshot by scrolling the page incrementally
    and assembling the viewport slices in memory using Pillow (PIL).

    Parameters
    ----------
    page:
        Active Playwright / Patchright page instance.
    max_slices:
        Maximum vertical slices to prevent runaway canvas on infinite scrolls.
    slice_delay_ms:
        Delay in milliseconds between scroll increments to let IntersectionObserver
        lazy images load and virtual DOM components mount.

    Returns
    -------
    Optional[bytes]:
        Compressed PNG bytes, or None if screenshotting failed.
    """
    try:
        from PIL import Image
    except ImportError:
        log.warning("[SCREENSHOT] Pillow is not installed; falling back to native screenshot.")
        try:
            return await page.screenshot(full_page=True, type="png")
        except Exception as exc:
            log.error("[SCREENSHOT] Native screenshot failed: %s", exc)
            return None

    try:
        # Get page and viewport dimensions
        dimensions = await page.evaluate(
            """() => {
                return {
                    width: Math.max(document.documentElement.clientWidth, window.innerWidth || 0),
                    height: Math.max(document.documentElement.clientHeight, window.innerHeight || 0),
                    scrollHeight: Math.max(
                        document.body.scrollHeight,
                        document.documentElement.scrollHeight,
                        document.body.offsetHeight,
                        document.documentElement.offsetHeight,
                        document.body.clientHeight,
                        document.documentElement.clientHeight
                    )
                };
            }"""
        )

        viewport_w = dimensions.get("width", 1280)
        viewport_h = dimensions.get("height", 800)
        scroll_h = dimensions.get("scrollHeight", 800)

        # If page is short, take standard viewport screenshot
        if scroll_h <= viewport_h:
            return await page.screenshot(type="png")

        # Calculate number of slices
        num_slices = min(max_slices, (scroll_h + viewport_h - 1) // viewport_h)
        slice_images = []

        # Scroll to top first
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(0.1)

        for slice_idx in range(num_slices):
            current_y = slice_idx * viewport_h
            await page.evaluate(f"window.scrollTo(0, {current_y})")
            await asyncio.sleep(slice_delay_ms / 1000.0)

            # On subsequent slices, suppress duplicate fixed/sticky navigation elements
            if slice_idx > 0:
                await page.evaluate(
                    """() => {
                        const fixedEls = document.querySelectorAll('*');
                        for (const el of fixedEls) {
                            const style = window.getComputedStyle(el);
                            if (style.position === 'fixed' || style.position === 'sticky') {
                                el.setAttribute('data-orig-visibility', el.style.visibility || '');
                                el.style.visibility = 'hidden';
                            }
                        }
                    }"""
                )

            # Capture slice
            slice_bytes = await page.screenshot(type="png")
            img = Image.open(io.BytesIO(slice_bytes))
            slice_images.append((img, current_y))

            # Restore fixed element visibility
            if slice_idx > 0:
                await page.evaluate(
                    """() => {
                        const fixedEls = document.querySelectorAll('[data-orig-visibility]');
                        for (const el of fixedEls) {
                            el.style.visibility = el.getAttribute('data-orig-visibility');
                            el.removeAttribute('data-orig-visibility');
                        }
                    }"""
                )

        if not slice_images:
            return None

        # Assemble slices on canvas
        first_img = slice_images[0][0]
        canvas_w = first_img.width
        # Final canvas height matches total scroll height captured
        total_captured_h = min(scroll_h, num_slices * viewport_h)
        stitched = Image.new("RGB", (canvas_w, total_captured_h), (255, 255, 255))

        for img, y_pos in slice_images:
            # If last slice overflows total height, crop excess
            if y_pos + img.height > total_captured_h:
                crop_h = total_captured_h - y_pos
                if crop_h > 0:
                    cropped = img.crop((0, 0, img.width, crop_h))
                    stitched.paste(cropped, (0, y_pos))
            else:
                stitched.paste(img, (0, y_pos))

        # Reset scroll to top
        await page.evaluate("window.scrollTo(0, 0)")

        # Export compressed PNG bytes
        out_buf = io.BytesIO()
        stitched.save(out_buf, format="PNG", optimize=True)
        return out_buf.getvalue()

    except Exception as exc:
        log.warning("[SCREENSHOT] Stitched screenshot failed (%s); falling back to single viewport.", exc)
        try:
            return await page.screenshot(type="png")
        except Exception:
            return None
