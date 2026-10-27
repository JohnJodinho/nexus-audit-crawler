"""First-party operator authentication endpoints."""

from __future__ import annotations

import asyncio
import logging
from typing import Literal

from fastapi import APIRouter, HTTPException, Request, Response, status
from pydantic import BaseModel, Field

from app.api.auth import (
    ACCESS_TOKEN_TTL_SECONDS,
    AuthConfigurationError,
    ensure_auth_configured,
    issue_access_token,
    verify_operator_credentials,
)
from app.api.auth_rate_limit import enforce_login_rate_limit

log = logging.getLogger("audit_crawler.api.auth")

router = APIRouter(prefix="/api/auth", tags=["Authentication"])


class LoginRequest(BaseModel):
    """Operator credentials submitted through the hosted console login form."""

    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=1024)


class LoginResponse(BaseModel):
    """Short-lived bearer token returned after successful login."""

    access_token: str
    token_type: Literal["bearer"] = "bearer"
    expires_in: int = ACCESS_TOKEN_TTL_SECONDS


def _authentication_unavailable(exc: AuthConfigurationError) -> HTTPException:
    log.error("%s", exc)
    return HTTPException(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        detail="API authentication is not configured.",
    )


@router.post("/login", response_model=LoginResponse)
async def login(
    credentials: LoginRequest,
    request: Request,
    response: Response,
) -> LoginResponse:
    """Verify the configured operator password and issue an in-memory-use token."""
    try:
        ensure_auth_configured()
        await enforce_login_rate_limit(request)
        subject = await asyncio.to_thread(
            verify_operator_credentials,
            credentials.username,
            credentials.password,
        )
        if subject is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid username or password.",
            )
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        return LoginResponse(access_token=issue_access_token(subject))
    except AuthConfigurationError as exc:
        raise _authentication_unavailable(exc) from exc
