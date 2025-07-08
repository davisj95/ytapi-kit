
import pandas as pd
from datetime import datetime
from ytapi_kit import user_session
import io
import re
from typing import Iterator

class ReportingClient:
    def __init__(self, session):
        self.session = session
        self.base_url = "https://youtubereporting.googleapis.com/v1"

    @staticmethod
    def _check_type(value, expected, name: str) -> None:
        """Raise TypeError if *value* is not None and not an *expected* type."""
        if value is None:
            return
        if not isinstance(value, expected):
            if isinstance(expected, tuple):
                exp = " or ".join(t.__name__ for t in expected)
            else:
                exp = expected.__name__
            raise TypeError(f"{name} must be {exp} | None")

    @staticmethod
    def _paged(func, *args, **kwargs) -> Iterator[pd.DataFrame]:
        """Yield successive DataFrame pages until the API returns no next token."""
        page, token = func(*args, **kwargs)
        yield page
        while token:
            page, token = func(*args, page_token=token, **kwargs)
            yield page

    def list_report_types(
            self,
            *,
            include_system_managed: bool | None = None,
            page_size: int | None = None,
            page_token: str | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """
        List available report types.

        Parameters
        ----------
        include_system_managed : bool, optional
            Whether to include YouTube system-managed report types.
        page_size : int, optional
            Maximum number of items to return.
        page_token : str, optional
            Token for fetching the next page of results.
        on_behalf_of_content_owner : str, optional
            CMS content-owner ID when acting on behalf of a partner account.

        Returns
        -------
        pandas.DataFrame
        """
        self._check_type(include_system_managed, bool, "include_system_managed")
        self._check_type(page_size, int, "page_size")
        self._check_type(page_token, str, "page_token")
        self._check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

        url = f"{self.base_url}/reportTypes"
        params: dict[str, object] = {}
        if include_system_managed is not None:
            params["includeSystemManaged"] = str(include_system_managed).lower()
        if page_size is not None:
            params["pageSize"] = page_size
        if page_token is not None:
            params["pageToken"] = page_token
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

        return payload.get("reportTypes", []), payload.get("nextPageToken")

    def create_job(
            self,
            *,
            report_type_id: str,
            name: str,
            on_behalf_of_content_owner: str | None = None,
    ) -> dict:
        """
        Create a reporting job.

        Parameters
        ----------
        report_type_id : str
            The type of report this job should create.
        name : str
            The name of the reporting job.
        on_behalf_of_content_owner : str, optional
            CMS content-owner ID when acting on behalf of a partner account.

        Returns
        -------
        pandas.DataFrame
        """
        self._check_type(report_type_id, str, "report_type_id")
        self._check_type(name, str, "name")
        self._check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

        url = f"{self.base_url}/jobs"
        params: dict[str, object] = {
            "name": name,
        }
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.post(url, params=params)
        resp.raise_for_status()
        return resp.json()

    def list_jobs(
            self,
            *,
            include_system_managed: bool | None = None,
            page_size: int | None = None,
            page_token: str | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """
        List existing Reporting API jobs.

        Parameters
        ----------
        include_system_managed : bool | None, default None
            Include jobs that YouTube has created for you
            (e.g., system-managed content-owner jobs).  ``None`` means
            “omit the query param and accept the API default (True).”
        page_size : int | None
            Max jobs per API call.
        page_token : str | None
            Token from a previous call to fetch the next page.
        on_behalf_of_content_owner : str | None
            CMS content-owner ID when acting on behalf of a partner.

        Returns
        -------
        (pandas.DataFrame, str | None)
            • DataFrame with columns ``id``, ``name``, ``reportTypeId``,
              ``createTime``, ``expireTime``, ``systemManaged``
            • ``next_page_token`` – ``None`` when there are no more pages.
        """
        self._check_type(include_system_managed, bool, "include_system_managed")
        self._check_type(page_size, int, "page_size")
        self._check_type(page_token, str, "page_token")
        self._check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

        url = f"{self.base_url}/jobs"
        params: dict[str, object] = {}
        if include_system_managed is not None:
            params["includeSystemManaged"] = str(include_system_managed).lower()
        if page_size:
            params["pageSize"] = page_size
        if page_token:
            params["pageToken"] = page_token
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()
        df = pd.DataFrame(payload.get("jobs", []))

        # cast timestamps to datetime64[ns, UTC]
        for ts in ("createTime", "expireTime"):
            df[ts] = pd.to_datetime(df[ts], errors="coerce", utc=True)

        return df, payload.get("nextPageToken")

    def get_job(
            self,
            job_id: str,
            *,
            on_behalf_of_content_owner: str | None = None,
    ) -> pd.DataFrame:
        """
        Get an existing Reporting API job.

        Parameters
        ----------
        job_id : str
            ID of an existing Reporting API job.
        on_behalf_of_content_owner : str | None
            CMS content-owner ID when acting on behalf of a partner.

        Returns
        -------
        (pandas.DataFrame, str | None)
            • DataFrame with columns ``id``, ``name``, ``reportTypeId``,
              ``createTime``, ``expireTime``, ``systemManaged``
            • ``next_page_token`` – ``None`` when there are no more pages.
        """
        if not isinstance(job_id, str) or not job_id:
            raise TypeError("job_id must be a non-empty str")
        if on_behalf_of_content_owner is not None and not isinstance(on_behalf_of_content_owner, str):
            raise TypeError("on_behalf_of_content_owner must be str | None")

        url = f"{self.base_url}/jobs/{job_id}"
        params: dict[str, object] = {}
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

        df = pd.DataFrame(payload)

        for col in ("id", "name", "reportTypeId",
                    "createTime", "expireTime", "systemManaged"):
            if col not in df.columns:
                df[col] = pd.NA

        for ts in ("createTime", "expireTime"):
            df[ts] = pd.to_datetime(df[ts], errors="coerce", utc=True)

        return df


    def delete_job(
            self,
            job_id: str,
            *,
            on_behalf_of_content_owner: str | None = None,
    ) -> None:
        """
        Delete an existing Reporting API job.

        Parameters
        ----------
        job_id : str
            ID of an existing Reporting API job.
        on_behalf_of_content_owner : str | None
            CMS content-owner ID when acting on behalf of a partner.

        Returns
        -------
        Returns nothing, but prints message saying the job was successfully deleted if
            200 or 204 response code is returned by the API.
        """
        if not isinstance(job_id, str) or not job_id:
            raise TypeError("job_id must be a non-empty str")
        if on_behalf_of_content_owner is not None and not isinstance(on_behalf_of_content_owner, str):
            raise TypeError("on_behalf_of_content_owner must be str | None")

        url = f"{self.base_url}/jobs/{job_id}"
        params: dict[str, object] = {}
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.delete(url, params=params)
        resp.raise_for_status()

        if resp.status_code in (200, 204):
            print(f"Job {job_id} successfully deleted.")

        return None

    def list_reports(
            self,
            job_id: str,
            *,
            page_size: int | None = None,
            page_token: str | None = None,
            created_after: datetime | str | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """
        List existing reports in a specific job.

        Parameters
        ----------
        job_id : str
            ID of an existing Reporting API job.
        page_size : int | None
            Max jobs per API call.
        page_token : str | None
            Token from a previous call to fetch the next page.
        created_after: datetime | str | None
            A datetime object or string representing the cutoff date of when reports are created.
        on_behalf_of_content_owner : str | None
            CMS content-owner ID when acting on behalf of a partner.

        Returns
        -------
        (pandas.DataFrame, str | None)
            • DataFrame with columns ``id``, ``jobId``, ``startTime``,
              ``endTime``, ``createTime``, ``downloadUrl``
            • ``next_page_token`` – ``None`` when there are no more pages.
        """
        if not isinstance(job_id, str) or not job_id:
            raise TypeError("job_id must be a non-empty str")
        if page_size is not None and not isinstance(page_size, int):
            raise TypeError("page_size must be int | None")
        if page_token is not None and not isinstance(page_token, str):
            raise TypeError("page_token must be str | None")
        if created_after is not None and not isinstance(created_after, (datetime, str)):
            raise TypeError("created_after must be datetime | RFC3339 str | None")
        if on_behalf_of_content_owner is not None and not isinstance(on_behalf_of_content_owner, str):
            raise TypeError("on_behalf_of_content_owner must be str | None")

        url = f"{self.base_url}/jobs/{job_id}/reports"
        params: dict[str, object] = {}
        if page_size:
            params["pageSize"] = page_size
        if page_token:
            params["pageToken"] = page_token
        if created_after:
            params["createdAfter"] = (
                created_after.isoformat(timespec="seconds").replace("+00:00", "Z")
                if isinstance(created_after, datetime) else created_after
            )
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        r = self.session.get(url, params=params)
        r.raise_for_status()
        payload = r.json()
        items = payload.get("reports", [])
        next_token = payload.get("nextPageToken")

        df = pd.DataFrame(items)
        for col in ("id", "startTime", "endTime", "createTime", "downloadUrl"):
            if col not in df.columns:
                df[col] = pd.NA
        for ts in ("startTime", "endTime", "createTime"):
            df[ts] = pd.to_datetime(df[ts], errors="coerce", utc=True)

        return df, next_token

    def get_reports(
            self,
            job_id: str,
            report_id: str,
            *,
            on_behalf_of_content_owner: str | None = None,
    ) -> pd.DataFrame:
        """
        List metadata for a specific report in a specific job.

        Parameters
        ----------
        job_id : str
            ID of an existing Reporting API job.
        report_id : str
            ID of an existing report within the specific job.
        on_behalf_of_content_owner : str | None
            CMS content-owner ID when acting on behalf of a partner.

        Returns
        -------
        pandas.DataFrame
            • DataFrame with columns ``id``, ``jobId``, ``startTime``,
              ``endTime``, ``createTime``, ``downloadUrl``
        """
        if not isinstance(job_id, str) or not job_id:
            raise TypeError("job_id must be a non-empty str")
        if not isinstance(report_id, str) or not report_id:
            raise TypeError("job_id must be a non-empty str")
        if on_behalf_of_content_owner is not None and not isinstance(on_behalf_of_content_owner, str):
            raise TypeError("on_behalf_of_content_owner must be str | None")

        url = f"{self.base_url}/jobs/{job_id}/reports/{report_id}"
        params: dict[str, object] = {}
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()
        items = payload.get("reports", [])

        df = pd.DataFrame(items)
        for col in ("id", "startTime", "endTime", "createTime", "downloadUrl"):
            if col not in df.columns:
                df[col] = pd.NA
        for ts in ("startTime", "endTime", "createTime"):
            df[ts] = pd.to_datetime(df[ts], errors="coerce", utc=True)

        return df

    def download_report(
            self,
            download_url: str,

    ) -> pd.DataFrame:
        """
        Download a report CSV and (optionally) return a typed DataFrame.

        Parameters
        ----------
        download_url : str
            HTTPS link from ``list_reports()``.

        Returns
        -------
        pandas.DataFrame | bytes
        """
        if not isinstance(download_url, str) or not download_url:
            raise TypeError("download_url must be a non-empty str")

        r = self.session.get(download_url, stream=True)
        r.raise_for_status()

        df = pd.read_csv(io.BytesIO(r.content))

           # --- datetime coercion ---
        date_like_cols = [
            c for c in df.columns
            if re.search(r"(day|date|month|time)$", c, re.IGNORECASE)
        ]
        for c in date_like_cols:
            df[c] = pd.to_datetime(df[c],  format="%Y%m%d")
        return df

    def get_latest_report(
            self,
            identifier: str,
    ) -> pd.DataFrame | bytes:
        """
        Download the *most recent* report in a single call.

        Parameters
        ----------
        identifier : str
            • A **reportTypeId** (e.g. ``"channel_basic_a2"``), *or*
            • A **job name** returned by ``list_jobs()`` (case-insensitive).

        Returns
        -------
        pandas.DataFrame | bytes
            Parsed DataFrame (default) or raw CSV bytes.
        """
        # ------- gather all jobs (pagination handled) -------
        jobs_df, next_tok = self.list_jobs(
            include_system_managed=True
        )
        while next_tok:
            page_df, next_tok = self.list_jobs(
                include_system_managed=True,
                page_token=next_tok
            )
            jobs_df = pd.concat([jobs_df, page_df], ignore_index=True)

        # ------- pick the job matching identifier -------
        mask_by_type = jobs_df["reportTypeId"].str.casefold() == identifier.casefold()
        mask_by_name = jobs_df["name"].str.casefold() == identifier.casefold()

        job_match = jobs_df[mask_by_type | mask_by_name]
        if job_match.empty:
            raise ValueError(f"No job found matching '{identifier}'")

        # If multiple jobs match, take the newest createTime
        job_match = job_match.sort_values("createTime", ascending=False)
        job_id = job_match.iloc[0]["id"]

        # ------- list all reports for that job -------
        reports_df, next_tok = self.list_reports(
            job_id,
        )
        while next_tok:
            page_df, next_tok = self.list_reports(
                job_id,
                page_token=next_tok,
            )
            reports_df = pd.concat([reports_df, page_df], ignore_index=True)

        if reports_df.empty:
            raise ValueError(f"No reports available for job '{identifier}'")

        # pick the most recent by startTime (falls back to createTime)
        reports_df = reports_df.sort_values(
            ["startTime", "createTime"], ascending=False, na_position="last"
        )
        latest = reports_df.iloc[0]
        download_url = reports_df["downloadUrl"].iloc[0]

        # ------- fetch CSV -------
        df = self.download_report(download_url)

        # ------- console banner -------
        start_str = pd.to_datetime(latest["startTime"]).date()
        print(f"{identifier} successfully downloaded for {start_str}")

        return df






if __name__ == "__main__":

    MainEng = user_session(
        "/Users/Jacob.davis95/client_secrets.json",
        token_cache="/Users/Jacob.davis95/.ytanalytics_token_single.pickle"
    )
    myBot = ReportingClient(MainEng)

    allReportsList, _ = myBot.list_report_types()
    allJobsList, _ = myBot.list_jobs()
    allJobReports, _ = myBot.list_reports(allJobsList["id"].iloc[5])
    finalReport = myBot.download_report(allJobReports["downloadUrl"].iloc[1])

    test = pd.to_datetime(finalReport["date"], format="%Y%m%d")

    testPull = myBot.get_latest_report("channel_basic_a2")





















