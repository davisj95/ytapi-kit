from __future__ import annotations

import re
from typing import Final, Iterator, Mapping, MutableMapping, Sequence
import pandas as pd
from datetime import datetime, date

from ._errors import *

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
        return _to_dataframe(payload.get("items", [])), payload.get("nextPageToken")




