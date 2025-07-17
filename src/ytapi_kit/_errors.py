from __future__ import annotations

"""ytapi_kit — **shared exception hierarchy** & HTTP‑error helper.

Every sub‑module (*_analytics.py*, *_reporting.py*, *_data.py*) should
`import` these classes so that callers can handle failures uniformly:

```python
from ytapi_kit._errors import QuotaExceeded, InvalidRequest

try:
    yt.reports_query(...)
except QuotaExceeded:
    sleep_until_midnight()
except InvalidRequest as e:
    logger.warning("bad parameter: %s", e)
```"""

from typing import Final

__all__ = [
    "YTAPIError",
    "QuotaExceeded",
    "RateLimited",
    "NotAuthorized",
    "Forbidden",
    "InvalidRequest",
    "raise_for_status",
]

# ---------------------------------------------------------------------------
# Base & specialised exceptions
# ---------------------------------------------------------------------------


class YTAPIError(Exception):
    """Base for *all* ytapi_kit exceptions."""


# ── Quota / rate‑limit ------------------------------------------------------
class QuotaExceeded(YTAPIError):
    """Daily project quota or per‑user quota exhausted (HTTP 403)."""


class RateLimited(YTAPIError):
    """Short‑term rate‑limit hit (HTTP 429 or 403 *userRateLimitExceeded*).

    The exception exposes ``retry_after`` seconds when available so callers can
    do exponential back‑off.
    """

    def __init__(self, message: str, retry_after: int | None = None):
        super().__init__(message)
        self.retry_after = retry_after


# ── Auth / permissions ------------------------------------------------------
class NotAuthorized(YTAPIError):
    """401 – invalid credentials or OAuth scope revoked."""


class Forbidden(YTAPIError):
    """403 – caller authenticated but not allowed to access the resource."""


# ── Client mistakes ---------------------------------------------------------
class InvalidRequest(YTAPIError):
    """400 / 404 – malformed query parameters or unknown resource ID."""


# ---------------------------------------------------------------------------
# Helper – map HTTP response → exception class
# ---------------------------------------------------------------------------


_QUOTA_REASONS: Final[set[str]] = {
    "quotaExceeded",
    "dailyLimitExceeded",
    "userRateLimitExceeded",
    "rateLimitExceeded",
}

_RATE_REASONS: Final[set[str]] = {
    "userRateLimitExceeded",
    "rateLimitExceeded",
}


def _reason(resp) -> str:  # noqa: ANN001
    """Return the *reason* field from Google’s error payload or ``"unknown"``."""
    try:
        return resp.json()["error"]["errors"][0]["reason"]
    except Exception:  # noqa: BLE001
        return "unknown"


def raise_for_status(resp) -> None:  # noqa: ANN001
    """Raise the appropriate *ytapi_kit* exception for *resp*.

    Does **nothing** when the response code is < 400.
    """
    if resp.status_code < 400:
        return

    reason = _reason(resp)
    message = f"YouTube API error {resp.status_code}: {resp.text}"

    # 401 ––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––––
    if resp.status_code == 401:
        raise NotAuthorized(message)

    # 403 – distinguish quota vs. generic forbidden  ––––––––––––––––––––
    if resp.status_code == 403:
        if reason in _QUOTA_REASONS:
            if reason in _RATE_REASONS:
                retry_after = int(resp.headers.get("Retry-After", "0") or 0)
                raise RateLimited(message, retry_after)
            raise QuotaExceeded(message)
        raise Forbidden(message)

    # 429 – explicit rate limit –––––––––––––––––––––––––––––––––––––––––
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "0") or 0)
        raise RateLimited(message, retry_after)

    # 400 / 404 – client errors ––––––––––––––––––––––––––––––––––––––––
    if resp.status_code in (400, 404):
        raise InvalidRequest(message)

    # Fallback – unknown 4xx/5xx –––––––––––––––––––––––––––––––––––––––
    raise YTAPIError(message)
