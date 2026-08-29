"""
tests/test_screenshots.py
=========================
Unit tests for the PIL stitched screenshot engine (Phase 3).
"""

import io
import pytest
from PIL import Image

from app.utils.screenshots import capture_stitched_screenshot


class MockPage:
    """Mock Playwright Page for screenshot unit testing."""

    def __init__(self, width: int = 1280, height: int = 800, scroll_height: int = 2400):
        self.width = width
        self.height = height
        self.scroll_height = scroll_height
        self.evaluations = []

    async def evaluate(self, script: str):
        self.evaluations.append(script)
        if "scrollHeight" in script:
            return {
                "width": self.width,
                "height": self.height,
                "scrollHeight": self.scroll_height,
            }
        return None

    async def screenshot(self, **kwargs):
        # Generate a synthetic PNG slice
        img = Image.new("RGB", (self.width, self.height), color=(100, 150, 200))
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()


@pytest.mark.asyncio
async def test_capture_stitched_screenshot_tall_page():
    mock_page = MockPage(width=1280, height=800, scroll_height=2400)
    shot_bytes = await capture_stitched_screenshot(mock_page, max_slices=5, slice_delay_ms=10)

    assert shot_bytes is not None
    assert len(shot_bytes) > 0

    # Verify Pillow can open the stitched image
    stitched_img = Image.open(io.BytesIO(shot_bytes))
    assert stitched_img.width == 1280
    assert stitched_img.height == 2400
    assert stitched_img.format == "PNG"


@pytest.mark.asyncio
async def test_capture_stitched_screenshot_short_page():
    mock_page = MockPage(width=1280, height=800, scroll_height=600)
    shot_bytes = await capture_stitched_screenshot(mock_page)

    assert shot_bytes is not None
    stitched_img = Image.open(io.BytesIO(shot_bytes))
    assert stitched_img.width == 1280
    assert stitched_img.height == 800
