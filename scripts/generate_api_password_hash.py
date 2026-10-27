"""Generate a scrypt hash for API_OPERATOR_PASSWORD_HASH."""

from __future__ import annotations

import getpass
import hmac

from app.api.auth import hash_operator_password


def main() -> None:
    password = getpass.getpass("Operator password (minimum 12 characters): ")
    confirmation = getpass.getpass("Confirm operator password: ")
    if not hmac.compare_digest(password, confirmation):
        raise SystemExit("Passwords do not match.")

    try:
        password_hash = hash_operator_password(password)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    print(password_hash)


if __name__ == "__main__":
    main()
