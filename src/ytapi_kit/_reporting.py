
import pandas as pd
from datetime import datetime
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

        return pd.DataFrame(payload.get("reportTypes", [])), payload.get("nextPageToken")

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
        self._check_type(report_type_id, str, "report_type_id")
        self._check_type(name, str, "name")
        self._check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

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
        pandas.DataFrame
            • DataFrame with columns ``id``, ``name``, ``reportTypeId``,
              ``createTime``, ``expireTime``, ``systemManaged``
        """
        self._check_type(job_id, str, "job_id")
        self._check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

        url = f"{self.base_url}/jobs/{job_id}"
        params: dict[str, object] = {}
        if on_behalf_of_content_owner:
            params["onBehalfOfContentOwner"] = on_behalf_of_content_owner

        resp = self.session.get(url, params=params)
        resp.raise_for_status()
        payload = resp.json()

        df = pd.DataFrame([payload])

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
        self._check_type(job_id, str, "job_id")
        self._check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

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
        self._check_type(job_id, str, "job_id")
        self._check_type(page_size, int, "page_size")
        self._check_type(page_token, str, "page_token")
        self._check_type(created_after, (datetime, str), "created_after")
        self._check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

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
        self._check_type(job_id, str, "job_id")
        self._check_type(report_id, str, "report_id")
        self._check_type(on_behalf_of_content_owner, str, "on_behalf_of_content_owner")

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
        self._check_type(download_url, str, "download_url")

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
        jobs_df = pd.concat(
            self._paged(self.list_jobs),
            ignore_index=True
        )

        mask = (jobs_df["reportTypeId"].str.casefold() == identifier.casefold()) | \
               (jobs_df["name"].str.casefold() == identifier.casefold())
        match = jobs_df.loc[mask]
        if match.empty:
            raise ValueError(f"No job found matching '{identifier}'")

        job_id = match.sort_values("createTime", ascending=False).iloc[0]["id"]

        reports_df = pd.concat(self._paged(self.list_reports, job_id), ignore_index=True)
        if reports_df.empty:
            raise ValueError(f"No reports available for job '{identifier}'")

        latest = reports_df.sort_values(
            ["startTime", "createTime"], ascending=False
        ).iloc[0]

        df = self.download_report(latest["downloadUrl"])
        print(f"{identifier} successfully downloaded for "
              f"{pd.to_datetime(latest['startTime']).date()}")
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
    testGetJob = myBot.get_job(allJobsList["id"].iloc[1])
    finalReport = myBot.download_report(allJobReports["downloadUrl"].iloc[1])

    testPull = myBot.get_latest_report("channel_device_os_a2")









