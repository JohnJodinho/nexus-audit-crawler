"""Redis-backed rate limiting for the public operator login endpoint."""

from __future__ import annotations

import hashlib
import logging

from fastapi import HTTPException, Request, status
from redis.exceptions import RedisError

from app.redis_client import create_redis_pool

log = logging.getLogger("audit_crawler.api.auth_rate_limit")

_ATTEMPT_LIMIT = 10
_WINDOW_SECONDS = 60
_INCREMENT_WITH_EXPIRY = """
local attempts = redis.call('INCR', KEYS[1])
if attempts == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[1])
end
return attempts
"""


async def enforce_login_rate_limit(request: Request) -> None:
    """Limit login attempts per connecting IP in a fixed one-minute window."""
    client_host = request.client.host if request.client else "unknown"
    address_digest = hashlib.sha256(client_host.encode("utf-8")).hexdigest()
    redis = create_redis_pool(max_connections=1)
    try:
        attempts = await redis.eval(
            _INCREMENT_WITH_EXPIRY,
            1,
            f"api:auth:login:{address_digest}",
            _WINDOW_SECONDS,
        )
    except RedisError as exc:
        log.exception("Redis-backed login rate limit failed")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Login is temporarily unavailable.",
        ) from exc
    finally:
        await redis.aclose()

    if attempts > _ATTEMPT_LIMIT:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Too many login attempts. Try again shortly.",
        )
