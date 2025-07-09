from __future__ import annotations

import pathlib, pickle
from typing import Final

from google.auth.credentials import Credentials as _BaseCreds
from google.auth.transport.requests import AuthorizedSession, Request as _AuthRequest
from google.oauth2.credentials import Credentials as _UserCreds
from google.oauth2.service_account import Credentials as _SvcCreds
from google_auth_oauthlib.flow import InstalledAppFlow
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

__all__ = [
    "user_session",
    "service_account_session"
]

SCOPES: Final[list[str]] = [
    "https://www.googleapis.com/auth/yt-analytics.readonly",
    "https://www.googleapis.com/auth/youtube.readonly",
]
DEFAULT_TOKEN_CACHE = pathlib.Path("~/.ytapi.pickle").expanduser()

def _load_user_credentials(client_secrets: pathlib.Path, cache_path: pathlib.Path) -> _UserCreds:
    """OAuth browser flow with local token caching."""
    creds: _UserCreds | None = None
    if cache_path.exists():
        creds = pickle.loads(cache_path.read_bytes())
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(_AuthRequest())
            cache_path.write_bytes(pickle.dumps(creds))

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(str(client_secrets), SCOPES)
        creds = flow.run_local_server(port=0)
        cache_path.write_bytes(pickle.dumps(creds))
    return creds

def _build_session(credentials: _BaseCreds, *, total: int = 5, backoff_factor: float = 0.5) -> AuthorizedSession:
    """Return an AuthorizedSession with a sensible retry policy."""
    session = AuthorizedSession(credentials)

    retry_policy = Retry(
        total=total,
        backoff_factor=backoff_factor,  # exponential back‑off 0.5→8s
        status_forcelist=[500, 502, 503, 504, 429],
        allowed_methods={"GET"},
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    for scheme in ("https://", "http://"):
        session.mount(scheme, adapter)
    return session

def user_session(
    client_secrets: str | pathlib.Path,
    *,
    token_cache: str | pathlib.Path | None = None,
) -> AuthorizedSession:
    """Create an AuthorizedSession via OAuth user flow."""
    client_secrets = pathlib.Path(client_secrets).expanduser()
    cache_path = pathlib.Path(token_cache).expanduser() if token_cache else DEFAULT_TOKEN_CACHE
    creds = _load_user_credentials(client_secrets, cache_path)
    return _build_session(creds)

def service_account_session(json_path: str | pathlib.Path) -> AuthorizedSession:
    """Create an AuthorizedSession from a service‑account key."""
    creds = _SvcCreds.from_service_account_file(str(pathlib.Path(json_path).expanduser()), scopes=SCOPES)
    return _build_session(creds)
