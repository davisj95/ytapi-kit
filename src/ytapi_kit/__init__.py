"""
ytapi_kit – high-level helpers for the YouTube Analytics / Data / Reporting APIs.

Import the public surface like so:

    from ytapi_kit import AnalyticsClient, user_session

Everything else (modules whose names start with “_”) is internal and
subject to change without notice.
"""

from importlib import metadata as _metadata

# ─────────────────────────────────────────────────────────────────────────────
# Re-export PUBLIC objects from the internal implementation modules
# (everything in _analytics.py is an implementation detail)
# ─────────────────────────────────────────────────────────────────────────────
from ._auth import user_session, service_account_session
from ._analytics import AnalyticsClient
from ._reporting import ReportingClient
from ._data import DataClient
from ._errors import (YTAPIError, QuotaExceeded, RateLimited, NotAuthorized,
                      Forbidden, InvalidRequest, raise_for_status)

__all__: list[str] = [
    "user_session",
    "service_account_session",
    "AnalyticsClient",
    "ReportingClient",
    "DataClient",
    "YTAPIError",
    "QuotaExceeded",
    "RateLimited",
    "NotAuthorized",
    "Forbidden",
    "InvalidRequest",
    "raise_for_status"
]

# ─────────────────────────────────────────────────────────────────────────────
# Version handling
# ─────────────────────────────────────────────────────────────────────────────
try:
    # Normal installed case – read version from package metadata
    __version__: str = _metadata.version(__name__)
except _metadata.PackageNotFoundError:
    # Editable/-e install while developing – fall back to __about__.py
    from .__about__ import __version__  # type: ignore[attr-defined]

# Clean up internal symbol so it doesn’t leak into dir(ytapi_kit)
del _metadata
