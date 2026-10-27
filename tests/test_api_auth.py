from __future__ import annotations

import datetime
import logging
from typing import Any
from unittest.mock import AsyncMock

import jwt
import pytest
from fastapi import HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials
from httpx import ASGITransport, AsyncClient
from redis.exceptions import RedisError

import app.api.auth as auth
import app.api.auth_rate_limit as auth_rate_limit
from app.api.app import create_app
from app.config import settings


@pytest.fixture
def configure_auth(monkeypatch):
    username = "operator"
    password = "test-operator-password-123"
    password_hash = auth.hash_operator_password(password)
    secret = "test-secret-key-with-at-least-32-characters"
    monkeypatch.setattr(settings, "API_OPERATOR_USERNAME", username)
    monkeypatch.setattr(settings, "API_OPERATOR_PASSWORD_HASH", password_hash)
    monkeypatch.setattr(settings, "API_TOKEN_SECRET", secret)
    return username, password, secret


def _issue_token(
    secret: str,
    *,
    subject: str = "operator",
    expires_delta: datetime.timedelta = datetime.timedelta(minutes=5),
    extra_claims: dict[str, Any] | None = None,
) -> str:
    now = datetime.datetime.now(datetime.UTC)
    claims: dict[str, Any] = {
        "iss": auth._TOKEN_ISSUER,
        "aud": auth._TOKEN_AUDIENCE,
        "sub": subject,
        "iat": now,
        "exp": now + expires_delta,
    }
    if extra_claims:
        claims.update(extra_claims)
    return jwt.encode(claims, secret, algorithm="HS256")


def _request() -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/api/crawls",
            "headers": [],
            "query_string": b"",
            "server": ("test", 80),
            "client": ("127.0.0.1", 1),
            "scheme": "http",
        }
    )


async def _authenticate(token: str | None):
    credentials = (
        HTTPAuthorizationCredentials(scheme="Bearer", credentials=token)
        if token is not None
        else None
    )
    return await auth.require_authenticated_principal(_request(), credentials)


@pytest.mark.asyncio
async def test_auth_accepts_fastapi_issued_operator_token(configure_auth):
    username, _, _ = configure_auth

    principal = await _authenticate(auth.issue_access_token(username))

    assert principal.subject == username
    assert principal.actor_type == "operator"


@pytest.mark.asyncio
async def test_auth_rejects_missing_malformed_expired_and_tampered_tokens(configure_auth):
    _, _, secret = configure_auth
    expired = _issue_token(
        secret,
        expires_delta=datetime.timedelta(seconds=-1),
    )
    valid_token = _issue_token(secret)
    altered_token = valid_token[:-1] + ("a" if valid_token[-1] != "a" else "b")

    for token in (None, "not-a-jwt", expired, altered_token):
        with pytest.raises(HTTPException) as response:
            await _authenticate(token)
        assert response.value.status_code == 401


@pytest.mark.asyncio
async def test_auth_rejects_wrong_subject_issuer_and_audience(configure_auth):
    _, _, secret = configure_auth
    wrong_subject = _issue_token(secret, subject="other-operator")
    wrong_issuer = _issue_token(secret, extra_claims={"iss": "https://untrusted.example"})
    wrong_audience = _issue_token(secret, extra_claims={"aud": "another-api"})

    for token in (wrong_subject, wrong_issuer, wrong_audience):
        with pytest.raises(HTTPException) as response:
            await _authenticate(token)
        assert response.value.status_code == 401


@pytest.mark.asyncio
async def test_auth_fails_closed_when_server_configuration_is_missing(monkeypatch):
    monkeypatch.setattr(settings, "API_OPERATOR_USERNAME", "")
    monkeypatch.setattr(settings, "API_OPERATOR_PASSWORD_HASH", "")
    monkeypatch.setattr(settings, "API_TOKEN_SECRET", "")

    with pytest.raises(HTTPException) as response:
        await _authenticate("signed-token")

    assert response.value.status_code == 503


@pytest.mark.asyncio
async def test_login_returns_short_lived_token_for_valid_credentials(
    configure_auth,
    monkeypatch,
):
    username, password, _ = configure_auth
    monkeypatch.setattr(
        "app.api.routes.auth.enforce_login_rate_limit",
        AsyncMock(return_value=None),
    )
    app = create_app()
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/auth/login",
            json={"username": username, "password": password},
        )

    assert response.status_code == 200
    result = response.json()
    assert result["token_type"] == "bearer"
    assert result["expires_in"] == 900
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["pragma"] == "no-cache"
    principal = await _authenticate(result["access_token"])
    assert principal.subject == username


@pytest.mark.asyncio
async def test_login_rejects_invalid_password_without_disclosing_username(
    configure_auth,
    monkeypatch,
):
    username, _, _ = configure_auth
    monkeypatch.setattr(
        "app.api.routes.auth.enforce_login_rate_limit",
        AsyncMock(return_value=None),
    )
    app = create_app()
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/auth/login",
            json={"username": username, "password": "wrong-password"},
        )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid username or password."

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        wrong_username = await client.post(
            "/api/auth/login",
            json={"username": "other-user", "password": "test-operator-password-123"},
        )

    assert wrong_username.status_code == 401
    assert wrong_username.json()["detail"] == "Invalid username or password."


@pytest.mark.asyncio
async def test_login_fails_closed_for_invalid_password_hash(monkeypatch):
    monkeypatch.setattr(settings, "API_OPERATOR_USERNAME", "operator")
    monkeypatch.setattr(settings, "API_OPERATOR_PASSWORD_HASH", "invalid-hash")
    monkeypatch.setattr(settings, "API_TOKEN_SECRET", "test-secret-key-with-at-least-32-characters")
    monkeypatch.setattr(
        "app.api.routes.auth.enforce_login_rate_limit",
        AsyncMock(return_value=None),
    )
    app = create_app()
    transport = ASGITransport(app=app)

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.post(
            "/api/auth/login",
            json={"username": "operator", "password": "any-password"},
        )

    assert response.status_code == 503


def test_password_hash_requires_a_strong_password():
    with pytest.raises(ValueError):
        auth.hash_operator_password("too-short")


@pytest.mark.asyncio
async def test_login_rate_limit_uses_hashed_client_address_and_rejects_excess(
    monkeypatch,
):
    redis = AsyncMock()
    redis.eval.return_value = 11
    monkeypatch.setattr(
        auth_rate_limit,
        "create_redis_pool",
        lambda **_kwargs: redis,
    )

    with pytest.raises(HTTPException) as response:
        await auth_rate_limit.enforce_login_rate_limit(_request())

    assert response.value.status_code == 429
    key = redis.eval.await_args.args[2]
    assert key.startswith("api:auth:login:")
    assert "127.0.0.1" not in key
    redis.aclose.assert_awaited_once()


@pytest.mark.asyncio
async def test_login_rate_limit_logs_redis_failure_and_fails_closed(
    monkeypatch,
    caplog,
):
    redis = AsyncMock()
    redis.eval.side_effect = RedisError("Upstash connection refused")
    monkeypatch.setattr(
        auth_rate_limit,
        "create_redis_pool",
        lambda **_kwargs: redis,
    )

    with caplog.at_level(
        logging.ERROR,
        logger="audit_crawler.api.auth_rate_limit",
    ):
        with pytest.raises(HTTPException) as response:
            await auth_rate_limit.enforce_login_rate_limit(_request())

    assert response.value.status_code == 503
    assert response.value.detail == "Login is temporarily unavailable."
    assert "Redis-backed login rate limit failed" in caplog.text
    assert "Upstash connection refused" in caplog.text
    redis.aclose.assert_awaited_once()
