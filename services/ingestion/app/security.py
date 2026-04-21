import hmac
import os
from typing import Optional

from fastapi import Header, HTTPException, status


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def require_api_key(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> None:
    """
    Simple shared-secret auth for internal/critical endpoints.

    Accepted forms:
    - X-API-Key: <key>
    - Authorization: Bearer <key>
    """
    if not _truthy(os.getenv("AUTH_ENABLED", "true")):
        return

    expected = os.getenv("WILDFIRE_API_KEY")
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Server auth is enabled but WILDFIRE_API_KEY is not configured",
        )

    presented = x_api_key
    if not presented and authorization:
        parts = authorization.split(" ", 1)
        if len(parts) == 2 and parts[0].lower() == "bearer":
            presented = parts[1].strip()

    if not presented or not hmac.compare_digest(presented, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Bearer"},
        )

