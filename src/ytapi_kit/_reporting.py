
import pandas as pd
from datetime import datetime
import io
import re
from typing import Iterator

from ._util import runtime_typecheck, _paged_list

class ReportingClient:
    def __init__(self, session):
        self.session = session
        self.base_url = "https://youtubereporting.googleapis.com/v1"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.session.close()


    @runtime_typecheck
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

        return pd.DataFrame(payload.get("reportTypes", [])), payload.get("nextPageToken")

    @runtime_typecheck
    def create_job(
            self,
            *,
            report_type_id: str,
            name: str | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> dict:
        """
        Create a reporting job.

        Parameters
        ----------
        report_type_id : str
            The type of report this job should create.
        name : str, optional
            The name of the reporting job.
        on_behalf_of_content_owner : str, optional
            CMS content-owner ID when acting on behalf of a partner account.

        Returns
        -------
        pandas.DataFrame
        """

        url = f"{self.base_url}/jobs"
        body = {
            "reportTypeId": report_type_id,
        }
        if name is not None:
            body["name"] = name
        params: dict[str, object] = {}
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.post(url, params=params, json=body)
        resp.raise_for_status()
        return resp.json()

    @runtime_typecheck
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

        return df, payload.get("nextPageToken")

    @runtime_typecheck
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
        pandas.DataFrame
            • DataFrame with columns ``id``, ``name``, ``reportTypeId``,
              ``createTime``, ``expireTime``, ``systemManaged``
        """

        url = f"{self.base_url}/jobs/{job_id}"
        params: dict[str, object] = {}
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

        df = pd.DataFrame([payload])

        return df

    @runtime_typecheck
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

        url = f"{self.base_url}/jobs/{job_id}"
        params: dict[str, object] = {}
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.delete(url, params=params)
        resp.raise_for_status()

        if resp.status_code in (200, 204):
            print(f"Job {job_id} successfully deleted.")

        return None

    @runtime_typecheck
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
        for ts in ("startTime", "endTime", "createTime"):
            df[ts] = pd.to_datetime(df[ts], errors="coerce", utc=True)

        return df, next_token

    @runtime_typecheck
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

        url = f"{self.base_url}/jobs/{job_id}/reports/{report_id}"
        params: dict[str, object] = {}
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

        df = pd.DataFrame([payload])
        for ts in ("startTime", "endTime", "createTime"):
            df[ts] = pd.to_datetime(df[ts], errors="coerce", utc=True)

        return df

    @runtime_typecheck
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

    @runtime_typecheck
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
        jobs_df = pd.concat(_paged_list(self.list_jobs), ignore_index=True)

        mask = (jobs_df["reportTypeId"].str.casefold() == identifier.casefold()) | \
               (jobs_df["name"].str.casefold() == identifier.casefold())
        match = jobs_df.loc[mask]
        if match.empty:
            raise ValueError(f"No job found matching '{identifier}'")

        job_id = match.sort_values("createTime", ascending=False).iloc[0]["id"]

        reports_df = pd.concat(_paged_list(self.list_reports, job_id), ignore_index=True)
        if reports_df.empty:
            raise ValueError(f"No reports available for job '{identifier}'")

        latest = reports_df.sort_values(
            ["startTime", "createTime"], ascending=False
        ).iloc[0]

        df = self.download_report(latest["downloadUrl"])
        print(f"{identifier} successfully downloaded for "
              f"{pd.to_datetime(latest['startTime']).date()}")
        return df












