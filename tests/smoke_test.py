"""Smoke tests for the audit crawler modules."""
import sys
sys.path.insert(0, '.')

# ---- Test extraction.py unit-level functions ----
from app.extraction import (
    extract_from_xhr, extract_from_hydration, extract_via_markitdown,
    _collect_keys_recursive,
)

# -------------------------------------------------------------------------
# Test 1: XHR extraction - relevant key match
# -------------------------------------------------------------------------
class FakeXHR:
    url = 'https://api.example.com/products'
    def json(self):
        return {'products': [{'name': 'Widget', 'price': 99}]}

result = extract_from_xhr([FakeXHR()])
assert result is not None and 'products' in result, f'XHR failed: {result}'
print('[TEST 1] XHR extraction with relevant key: PASS')

# -------------------------------------------------------------------------
# Test 2: XHR extraction - irrelevant key (should return None)
# -------------------------------------------------------------------------
class FakeXHRIrrelevant:
    url = 'https://cdn.example.com/config'
    def json(self):
        return {'version': '1.0', 'build': '12345', 'debug': False}

result2 = extract_from_xhr([FakeXHRIrrelevant()])
assert result2 is None, f'XHR relevance filter failed: {result2}'
print('[TEST 2] XHR extraction with irrelevant key: PASS (correctly None)')

# -------------------------------------------------------------------------
# Test 3: Hydration extraction - Next.js __NEXT_DATA__
# -------------------------------------------------------------------------
next_html = (
    '<html><script id="__NEXT_DATA__" type="application/json">'
    '{"props":{"pageProps":{"title":"Test","pricing":{"monthly":99}}}}'
    '</script></html>'
)
result3 = extract_from_hydration(next_html)
assert result3 is not None and 'props' in result3, f'Hydration failed: {result3}'
print('[TEST 3] Hydration extraction (__NEXT_DATA__): PASS')

# -------------------------------------------------------------------------
# Test 4: MarkItDown conversion
# -------------------------------------------------------------------------
html_bytes = b'<html><body><h1>Enterprise AI</h1><p>Our pricing starts at $99/mo.</p></body></html>'
result4 = extract_via_markitdown(html_bytes)
assert result4 is not None and 'Enterprise AI' in result4, f'MarkItDown failed: {result4}'
print('[TEST 4] MarkItDown HTML->Markdown: PASS')

# -------------------------------------------------------------------------
# Test 5: URL boundary patterns
# -------------------------------------------------------------------------
from app.spider import ALLOW_PATTERN, DENY_PATTERN

assert ALLOW_PATTERN.search('/about'), 'ALLOW /about failed'
assert ALLOW_PATTERN.search('/pricing'), 'ALLOW /pricing failed'
assert ALLOW_PATTERN.search('/services'), 'ALLOW /services failed'
assert ALLOW_PATTERN.search('/case-studies'), 'ALLOW /case-studies failed'
assert DENY_PATTERN.search('/blog'), 'DENY /blog failed'
assert DENY_PATTERN.search('/privacy'), 'DENY /privacy failed'
assert DENY_PATTERN.search('/login'), 'DENY /login failed'
assert DENY_PATTERN.search('/careers'), 'DENY /careers failed'
assert not ALLOW_PATTERN.search('/random-page'), '/random-page should not match ALLOW'
assert not DENY_PATTERN.search('/about'), '/about should not match DENY'
print('[TEST 5] URL boundary patterns (ALLOW/DENY): PASS')

# -------------------------------------------------------------------------
# Test 6: Recursive key collector
# -------------------------------------------------------------------------
obj = {'a': {'b': {'price': 100, 'name': 'test'}}, 'items': [{'c': 1}]}
keys = _collect_keys_recursive(obj)
assert 'price' in keys and 'name' in keys and 'items' in keys, f'Key collector: {keys}'
print('[TEST 6] _collect_keys_recursive: PASS')

# -------------------------------------------------------------------------
# Test 7: Logger module
# -------------------------------------------------------------------------
import logging
from app.logger import LOG_FILE_PATH, get_pipeline_logger, attach_file_handler
assert 'crawler.log' in LOG_FILE_PATH
pipeline_log = get_pipeline_logger('test.pipeline')
assert isinstance(pipeline_log, logging.Logger)
print('[TEST 7] Logger module: PASS')

print()
print('All smoke tests PASSED.')
