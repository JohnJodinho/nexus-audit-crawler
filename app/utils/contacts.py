"""
app/utils/contacts.py
=====================
High-precision, false-positive-resistant contact extraction and validation.

Extracts and validates emails and phone numbers from both href attributes
(mailto:, tel:) and plain text/Markdown bodies.

Defenses against false positives:
---------------------------------
1. Emails:
   - Rejects image/font retina asset strings (e.g. ``logo@2x.png``).
   - Rejects telemetry/error tracking addresses (e.g. Sentry ingest hashes).
   - Rejects numeric hex/UUID prefixes with ``@``.
   - Requires valid TLD (2-24 alpha characters) and clean domain structure.
   - Enforces length limits (6-254 total, local-part <= 64).

2. Phone Numbers:
   - Requires valid international or national structured formatting.
   - Enforces ITU-T E.164 digit bounds (7 to 15 digits).
   - Rejects dates, timestamps, IP addresses, postal codes, and pure numeric sequences.
   - Rejects dummy/sequential digit patterns (e.g. ``123456789``, ``00000000``).
   - Strips noise punctuation while preserving meaningful country codes.
"""

from __future__ import annotations

import re
from typing import Dict, List, Set
from urllib.parse import unquote

# ---------------------------------------------------------------------------
# Email Validation Patterns
# ---------------------------------------------------------------------------

_EMAIL_REGEX: re.Pattern[str] = re.compile(
    r"\b[A-Za-z0-9](?:[A-Za-z0-9._%+-]{0,62}[A-Za-z0-9])?"
    r"@(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?\.)+"
    r"[A-Za-z]{2,24}\b"
)

_INVALID_EMAIL_EXTENSIONS: frozenset[str] = frozenset({
    "png", "jpg", "jpeg", "gif", "svg", "webp", "ico", "bmp", "tiff",
    "woff", "woff2", "ttf", "eot", "otf",
    "css", "js", "json", "xml", "map", "ts",
    "mp4", "webm", "mp3", "wav", "pdf", "zip", "gz", "tar",
})

_DISALLOWED_EMAIL_DOMAINS: frozenset[str] = frozenset({
    "example.com", "example.org", "example.net",
    "domain.com", "yourdomain.com", "yoursite.com",
    "test.com", "localhost", "local",
    "sentry.io", "ingest.sentry.io", "w3.org",
})

_UUID_OR_HEX_PATTERN: re.Pattern[str] = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def validate_email(email: str) -> str | None:
    """
    Validate and clean an extracted email address.

    Returns the lowercase normalized email if valid, or ``None`` if rejected.
    """
    if not email:
        return None

    clean = unquote(email).strip().strip(".,;:()<>[]\"'").lower()

    if len(clean) < 6 or len(clean) > 254:
        return None

    if "@" not in clean:
        return None

    local_part, _, domain_part = clean.partition("@")

    if not local_part or not domain_part or len(local_part) > 64:
        return None

    if local_part.startswith(".") or local_part.endswith("."):
        return None

    if ".." in local_part or ".." in domain_part:
        return None

    # Reject retina asset filenames e.g. "image@2x.png"
    if "." in domain_part:
        ext = domain_part.rsplit(".", 1)[1].lower()
        if ext in _INVALID_EMAIL_EXTENSIONS:
            return None

    # Reject known placeholder / ingest domains
    if domain_part in _DISALLOWED_EMAIL_DOMAINS:
        return None

    # Reject UUID-like local parts
    if _UUID_OR_HEX_PATTERN.match(local_part):
        return None

    # Final regex verification
    if not _EMAIL_REGEX.fullmatch(clean):
        return None

    return clean


# ---------------------------------------------------------------------------
# Phone Number Validation Patterns
# ---------------------------------------------------------------------------

# Matches international and structured national phone number patterns:
# e.g., +65 6016 8637, +1 (555) 123-4567, +44 20 7946 0958, (555) 123-4567, +1-800-555-0199
_PHONE_REGEX: re.Pattern[str] = re.compile(
    r"(?:(?:\+|00)\d{1,3}[-.\s]?)?"                      # Optional international code (+65, 001)
    r"(?:\(?\d{1,4}\)?[-.\s]?)?"                         # Optional area code ((020), (555))
    r"\d{3,4}[-.\s]?\d{3,5}\b"                          # Core subscriber number
)

_DATE_PATTERN: re.Pattern[str] = re.compile(
    r"^\d{4}[-/.]\d{2}[-/.]\d{2}$|^\d{2}[-/.]\d{2}[-/.]\d{4}$"
)
_TIME_PATTERN: re.Pattern[str] = re.compile(r"^\d{1,2}:\d{2}(?::\d{2})?$")
_IP_PATTERN: re.Pattern[str] = re.compile(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$")


def validate_phone(phone: str) -> str | None:
    """
    Validate and normalize an extracted phone number candidate.

    Rejects dates, times, IP addresses, postal codes, and numbers outside
    the valid E.164 length range (7 to 15 digits).

    Returns a clean standardized string or ``None``.
    """
    if not phone:
        return None

    raw = unquote(phone).strip().strip(".,;:<>\"'#*")

    # Reject dates, times, IP addresses
    if _DATE_PATTERN.match(raw) or _TIME_PATTERN.match(raw) or _IP_PATTERN.match(raw):
        return None

    digits = re.sub(r"\D", "", raw)
    num_digits = len(digits)

    # Valid E.164 phone numbers have 7 to 15 digits
    if num_digits < 7 or num_digits > 15:
        return None

    # Reject all identical digits e.g. 00000000, 11111111
    if len(set(digits)) == 1:
        return None

    # Reject simple ascending or descending sequences e.g. 12345678, 98765432
    if digits in "01234567890123456789" or digits in "98765432109876543210":
        return None

    # Reject bare digit strings with no phone formatting (e.g. Unix timestamps, entity IDs)
    # Legitimate phone numbers in text are prefixed with '+' or contain separators (spaces, hyphens, parens)
    has_phone_structure = bool(re.search(r"^[+]|[\s\-().]", raw))
    if not has_phone_structure:
        return None

    # Reject Unix epoch timestamps (seconds or milliseconds starting with 15, 16, 17, 18, 19)
    if (num_digits in (10, 13)) and digits.startswith(("15", "16", "17", "18", "19")) and not raw.startswith("+"):
        return None

    # Normalize whitespace/separators
    clean = re.sub(r"[\s.-]+", " ", raw).strip()
    return clean


# ---------------------------------------------------------------------------
# High-Level Text Extractors
# ---------------------------------------------------------------------------

def extract_contacts_from_text(text: str) -> Dict[str, List[str]]:
    """
    Scan plain text or Markdown for valid emails and phone numbers.

    Parameters
    ----------
    text:
        Raw text or Markdown document to scan.

    Returns
    -------
    dict
        ``{"emails": [...], "phones": [...]}`` sorted and deduplicated.
    """
    if not text:
        return {"emails": [], "phones": []}

    found_emails: Set[str] = set()
    found_phones: Set[str] = set()

    # 1. Extract and validate emails
    for match in _EMAIL_REGEX.finditer(text):
        candidate = match.group(0)
        valid = validate_email(candidate)
        if valid:
            found_emails.add(valid)

    # 2. Extract and validate phone numbers
    for match in _PHONE_REGEX.finditer(text):
        candidate = match.group(0)
        valid = validate_phone(candidate)
        if valid:
            found_phones.add(valid)

    return {
        "emails": sorted(found_emails),
        "phones": sorted(found_phones),
    }
