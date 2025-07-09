from __future__ import annotations

import re
from typing import Mapping, MutableMapping, Sequence
import pandas as pd
from datetime import datetime, date

from ._errors import *
from ._util import _check_type, _string_to_tuple, _raise_invalid_argument

class DataClient:

    def __init__(self, session):
        self.session = session
        self.base_url = "https://www.googleapis.com/youtube/v3/"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    @staticmethod
    def _iso(dt: datetime | date | str) -> str:  # helper
        if isinstance(dt, (datetime, date)):
            return dt.isoformat(timespec="seconds") + ("Z" if isinstance(dt, datetime) and dt.tzinfo is None else "")
        return dt

    @staticmethod
    def _to_dataframe(items: Sequence[Mapping]) -> pd.DataFrame:
        """Flatten the *items* list returned by most v3 endpoints."""
        if not items:
            return pd.DataFrame()

        df = pd.json_normalize(items, sep=".")

        # Try to coerce any ISO date-time strings
        dt_like = [c for c in df.columns if re.search(r"(date|time|At)$", c, re.IGNORECASE)]
        for c in dt_like:
            df[c] = pd.to_datetime(df[c], errors="ignore", utc=True)

        return df

    def data_request(
            self,
            method: str,
            path: str,
            params: MutableMapping[str, object] | None = None,
            *,
            json_data: Mapping | None = None,
            files: Mapping[str, tuple] | None = None,
            stream: bool = False,
    ):
        """Issue an HTTP *method* request; return parsed JSON or raw *Response*."""
        url = f"{self.base_url}{path}"
        resp = self.session.request(
            method.upper(), url, params=params or {}, json=json_data, files=files, stream=stream, timeout=60
        )

        raise_for_status(resp)

        return resp if stream else resp.json()

    def _list_helper(
            self,
            resource: str,
            *,
            params: Mapping[str, object],
    ) -> tuple[pd.DataFrame, str | None]:
        payload = self.data_request("GET", f"/{resource}", dict(params))
        return self._to_dataframe(payload.get("items", [])), payload.get("nextPageToken")

    def list_activities(
            self,
            *,
            channel_id: str | None = None,
            mine: bool | None = None,
            part: str | Sequence[str] = ("contentDetails", "snippet"),
            published_after: datetime | date | str | None = None,
            published_before: datetime | date | str | None = None,
            region_code: str | None = None,
            page_token: str | None = None,
            max_results: int | None = 50,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for **activities.list**.

        Google reference → https://developers.google.com/youtube/v3/docs/activities/list
        """
        if sum(map(bool, (channel_id, mine))) != 1:
            raise ValueError("Supply exactly one of channel_id, home=True, or mine=True.")

        _check_type(channel_id, str, "channel_id")
        _check_type(mine, bool, "mine")
        _check_type(part, (str, Sequence), "part")
        _check_type(published_after, (datetime, date, str), "published_after")
        _check_type(published_before, (datetime, date, str), "published_before")
        _check_type(region_code, str, "region_code")
        _check_type(page_token, str, "page_token")
        _check_type(max_results, int, "max_results")

        _ACTIVITY_PARTS_ALLOWED = {"contentDetails", "id", "snippet"}

        parts_tuple = _string_to_tuple(part)
        if not set(parts_tuple).issubset(_ACTIVITY_PARTS_ALLOWED):
            _raise_invalid_argument("part", part, _ACTIVITY_PARTS_ALLOWED)
        params: dict[str, object] = {"part": ",".join(parts_tuple)}

        if channel_id:
            params["channelId"] = channel_id
        if mine is not None:
            params["mine"] = str(mine).lower()

        if published_after:
            params["publishedAfter"] = self._iso(published_after)
        if published_before:
            params["publishedBefore"] = self._iso(published_before)
        if region_code:
            params["regionCode"] = region_code
        if page_token:
            params["pageToken"] = page_token
        if max_results is not None:
            params["maxResults"] = max_results

        return self._list_helper("activities", params=params)

    def list_captions(
            self,
            *,
            part: str | Sequence[str] = ("id", "snippet"),
            video_id: str,
            id: str | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for **captions.list**."""

        _check_type(part, (str, Sequence), "part")
        _check_type(video_id, str, "video_id")
        _check_type(id, str, "id")
        _check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

        _CAPTION_PARTS_ALLOWED = {"id", "snippet"}

        parts_tuple = _string_to_tuple(part)
        if not set(parts_tuple).issubset(_CAPTION_PARTS_ALLOWED):
            _raise_invalid_argument("part", part, _CAPTION_PARTS_ALLOWED)
        params: dict[str, object] = {
            "part": ",".join(parts_tuple),
            "videoId": video_id,
        }

        if id:
            params["id"] = id
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        df, _ = self.list_helper("captions", params=params)
        return df


