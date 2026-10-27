"""First-party password verification and short-lived API token validation."""

from __future__ import annotations

import base64
import binascii
import datetime
import hashlib
import hmac
import logging
import secrets
from dataclasses import dataclass
from typing import Any

import jwt
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings

log = logging.getLogger("audit_crawler.api.auth")

_TOKEN_ALGORITHM = "HS256"
_TOKEN_ISSUER = "nexus-audit-crawler-api"
_TOKEN_AUDIENCE = "nexus-management-console"
ACCESS_TOKEN_TTL_SECONDS = 900
_SCRYPT_N = 2**14
_SCRYPT_R = 8
_SCRYPT_P = 1
_SCRYPT_SALT_BYTES = 16
_SCRYPT_KEY_BYTES = 64
_bearer_scheme = HTTPBearer(auto_error=False, scheme_name="BearerAuth")


@dataclass(frozen=True)
class Principal:
    """Authenticated operator allowed to use the management API."""

    subject: str
    actor_type: str = "operator"


class AuthConfigurationError(Exception):
    """Raised when required first-party authentication settings are invalid."""


def hash_operator_password(password: str) -> str:
    """Create a scrypt password hash for the operator environment setting."""
    if len(password) < 12:
        raise ValueError("Operator passwords must contain at least 12 characters.")

    salt = secrets.token_bytes(_SCRYPT_SALT_BYTES)
    digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_SCRYPT_KEY_BYTES,
    )
    encode = lambda value: base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")
    return f"scrypt${encode(salt)}${encode(digest)}"


def _decode_password_hash(password_hash: str) -> tuple[bytes, bytes]:
    try:
        algorithm, encoded_salt, encoded_digest = password_hash.split("$")
        if algorithm != "scrypt":
            raise ValueError
        decode = lambda value: base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
        salt = decode(encoded_salt)
        expected_digest = decode(encoded_digest)
    except (ValueError, binascii.Error) as exc:
        raise AuthConfigurationError("The configured operator password hash is invalid.") from exc

    if len(salt) != _SCRYPT_SALT_BYTES or len(expected_digest) != _SCRYPT_KEY_BYTES:
        raise AuthConfigurationError("The configured operator password hash is invalid.")
    return salt, expected_digest


def _configured_credentials() -> tuple[str, str, str]:
    username = settings.API_OPERATOR_USERNAME
    password_hash = settings.API_OPERATOR_PASSWORD_HASH
    token_secret = settings.API_TOKEN_SECRET
    if not username or not password_hash or len(token_secret.encode("utf-8")) < 32:
        log.error("Operator credentials and a 32-character token secret must be configured.")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="API authentication is not configured.",
        )
    return username, password_hash, token_secret


def ensure_auth_configured() -> None:
    """Fail closed if the operator credentials or signing secret are absent."""
    _configured_credentials()


def _password_matches(password: str, password_hash: str) -> bool:
    salt, expected_digest = _decode_password_hash(password_hash)
    actual_digest = hashlib.scrypt(
        password.encode("utf-8"),
        salt=salt,
        n=_SCRYPT_N,
        r=_SCRYPT_R,
        p=_SCRYPT_P,
        dklen=_SCRYPT_KEY_BYTES,
    )
    return hmac.compare_digest(actual_digest, expected_digest)


def verify_operator_credentials(username: str, password: str) -> str | None:
    """Return the configured operator name only when both credentials match."""
    configured_username, password_hash, _ = _configured_credentials()
    password_matches = _password_matches(password, password_hash)
    username_matches = hmac.compare_digest(username, configured_username)
    if username_matches and password_matches:
        return configured_username
    return None


def issue_access_token(subject: str) -> str:
    """Sign a short-lived access token for the configured operator."""
    _, _, token_secret = _configured_credentials()
    now = datetime.datetime.now(datetime.UTC)
    return jwt.encode(
        {
            "iss": _TOKEN_ISSUER,
            "aud": _TOKEN_AUDIENCE,
            "sub": subject,
            "iat": now,
            "exp": now + datetime.timedelta(seconds=ACCESS_TOKEN_TTL_SECONDS),
        },
        token_secret,
        algorithm=_TOKEN_ALGORITHM,
    )


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid or missing bearer token.",
        headers={"WWW-Authenticate": "Bearer"},
    )


def _verified_claims(token: str) -> tuple[str, dict[str, Any]]:
    try:
        configured_username, _, token_secret = _configured_credentials()
        claims: dict[str, Any] = jwt.decode(
            token,
            token_secret,
            algorithms=[_TOKEN_ALGORITHM],
            audience=_TOKEN_AUDIENCE,
            issuer=_TOKEN_ISSUER,
            options={"require": ["exp", "iat", "iss", "aud", "sub"]},
        )
    except HTTPException:
        raise
    except jwt.PyJWTError as exc:
        raise _unauthorized() from exc
    return configured_username, claims


def _operator_principal(claims: dict[str, Any], configured_username: str) -> Principal:
    subject = claims.get("sub")
    if not isinstance(subject, str) or not hmac.compare_digest(subject, configured_username):
        raise _unauthorized()
    return Principal(subject=subject)


async def require_authenticated_principal(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
) -> Principal:
    """Verify a FastAPI-issued bearer token and attach its operator principal."""
    token = credentials.credentials if credentials and credentials.scheme.lower() == "bearer" else None
    if token is None:
        raise _unauthorized()

    configured_username, claims = _verified_claims(token)
    principal = _operator_principal(claims, configured_username)
    request.state.principal = principal
    return principal
