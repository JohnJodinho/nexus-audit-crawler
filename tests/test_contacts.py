"""
tests/test_contacts.py
======================
Comprehensive test suite for high-precision contact extraction and false-positive filtering.
"""

import pytest
from app.utils.contacts import (
    validate_email,
    validate_phone,
    extract_contacts_from_text,
)


class TestValidateEmail:
    def test_valid_standard_emails(self):
        assert validate_email("john.doe@example.co.uk") == "john.doe@example.co.uk"
        assert validate_email("danny.ong@setialaw.com") == "danny.ong@setialaw.com"
        assert validate_email("support_team+123@sub.domain.org") == "support_team+123@sub.domain.org"

    def test_reject_retina_image_assets(self):
        """Retina image assets like logo@2x.png must be rejected."""
        assert validate_email("logo@2x.png") is None
        assert validate_email("icon@3x.jpg") is None
        assert validate_email("banner@2x.webp") is None
        assert validate_email("font@v1.woff2") is None

    def test_reject_uuids_and_hex_hashes(self):
        """UUIDs and long hex hashes with @ must be rejected."""
        assert validate_email("123e4567-e89b-12d3-a456-426614174000@domain.com") is None
        assert validate_email("c3983294-3948-4392-a1b2-123456789abc@something.com") is None

    def test_reject_disallowed_placeholder_domains(self):
        """Placeholder/dummy domains must be rejected."""
        assert validate_email("test@example.com") is None
        assert validate_email("user@yourdomain.com") is None
        assert validate_email("admin@localhost") is None

    def test_reject_malformed_emails(self):
        assert validate_email("not-an-email") is None
        assert validate_email("@domain.com") is None
        assert validate_email("user@") is None
        assert validate_email("user@.com") is None
        assert validate_email(".user@domain.com") is None
        assert validate_email("user..name@domain.com") is None


class TestValidatePhone:
    def test_valid_international_formats(self):
        assert validate_phone("+65 6016 8637") is not None
        assert validate_phone("+1 (555) 123-4567") is not None
        assert validate_phone("+44 20 7946 0958") is not None
        assert validate_phone("+65-6016-8637") is not None

    def test_valid_structured_national_formats(self):
        assert validate_phone("(555) 123-4567") is not None
        assert validate_phone("020 7946 0958") is not None

    def test_reject_dates_and_timestamps(self):
        """Dates and timestamps must never be classified as phone numbers."""
        assert validate_phone("2026-08-29") is None
        assert validate_phone("29/08/2026") is None
        assert validate_phone("12:30:45") is None

    def test_reject_ip_addresses(self):
        """IP addresses must not be classified as phone numbers."""
        assert validate_phone("192.168.1.1") is None
        assert validate_phone("127.0.0.1") is None

    def test_reject_sequential_or_repeated_digits(self):
        """Dummy/test numbers must be rejected."""
        assert validate_phone("00000000") is None
        assert validate_phone("111111111") is None
        assert validate_phone("123456789") is None

    def test_reject_short_numbers_and_postal_codes(self):
        """Short codes and postal codes must be rejected."""
        assert validate_phone("049145") is None
        assert validate_phone("90210") is None
        assert validate_phone("12345") is None


class TestExtractContactsFromText:
    def test_extract_mixed_text(self):
        sample_markdown = """
        # Contact Us

        For enquiries, reach out to our team:
        **Email:** enquiries@setialaw.com
        **Telephone:** +65 6016 8637
        **Address:** One George Street, #07-03, Singapore 049145

        Also check out our asset at /images/logo@2x.png (not an email).
        Published on: 2026-08-29
        Server IP: 192.168.1.100
        """

        contacts = extract_contacts_from_text(sample_markdown)

        assert "enquiries@setialaw.com" in contacts["emails"]
        assert len(contacts["emails"]) == 1
        assert "logo@2x.png" not in contacts["emails"]

        assert any("6016 8637" in p for p in contacts["phones"])
        assert not any("2026" in p for p in contacts["phones"])
        assert not any("192.168" in p for p in contacts["phones"])
        assert not any("049145" in p for p in contacts["phones"])
