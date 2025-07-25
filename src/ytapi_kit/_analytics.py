from __future__ import annotations

from datetime import date
from typing import Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from google.auth.transport.requests import AuthorizedSession

from ._errors import *
from ._util import *

# Possible Dimensions ---------------------------------------------------------
RESOURCE_DIMENSIONS = {"video", "playlist", "channel"}
GEOGRAPHIC_DIMENSIONS = {"country", "province", "dma", "city"}
TIME_PERIOD_DIMENSIONS = {"day", "month"}
PLAYBACK_LOCATION_DIMENSIONS = {"insightPlaybackLocationType",
                                "insightPlaybackLocationDetail"}
PLAYBACK_DETAIL_DIMENSIONS = {"creatorContentType","liveOrOnDemand",
                              "subscribedStatus","youtubeProduct"}
TRAFFIC_SOURCE_DIMENSIONS = {"insightTrafficSourceType",
                             "insightTrafficSourceDetail"}
DEVICE_DIMENSIONS = {"deviceType", "operatingSystem"}
DEMOGRAPHIC_DIMENSIONS = {"ageGroup", "gender"}
CONTENT_SHARING_DIMENSIONS = {"sharingService"}
AUDIENCE_RETENTION_DIMENSIONS = {"elapsedVideoTimeRatio"}
LIVESTREAM_DIMENSIONS = {"livestreamPosition"}
MEMBERSHIP_CANCELLATION_DIMENSIONS = {"membershipsCancellationSurveyReason"}
AD_PERFORMANCE_DIMENSIONS = {"adType"}

# Possible Metrics ------------------------------------------------------------
VIEW_METRICS = {"engagedViews", "views", "playlistViews", "redViews", "viewerPercentage"}
WATCH_TIME_METRICS = {"estimatedMinutesWatched", "estimatedRedMinutesWatched",
                      "averageViewDuration", "averageViewPercentage"}
ENGAGEMENT_METRICS = {"comments", "likes", "dislikes", "shares",
                      "subscribersGained", "subscribersLost",
                      "videosAddedToPlaylists", "videosRemovedFromPlaylists"}
PLAYLIST_METRICS = {"averageTimeInPlaylist", "playlistAverageViewDuration",
                    "playlistEstimatedMinutesWatched", "playlistSaves",
                    "playlistStarts", "playlistViews", "viewsPerPlaylistStart"}
ANNOTATION_METRICS = {"annotationImpressions", "annotationClickableImpressions",
                      "annotationClicks", "annotationClickThroughRate",
                      "annotationClosableImpressions", "annotationCloses",
                      "annotationCloseRate"}
CARD_METRICS = {"cardImpressions", "cardClicks", "cardClickRate",
                "cardTeaserImpressions", "cardTeaserClicks", "cardTeaserClickRate"}
LIVESTREAM_METRICS = {"averageConcurrentViewers", "peakConcurrentViewers"}
AUDIENCE_RETENTION_METRICS = {"audienceWatchRatio", "relativeRetentionPerformance",
                              "startedWatching","stoppedWatching",
                              "totalSegmentImpressions"}
MEMBERSHIP_CANCELLATION_METRICS = {"membershipsCancellationSurveyResponses"}
ESTIMATED_REVENUE_METRICS = {"estimatedRevenue", "estimatedAdRevenue",
                             "estimatedRedPartnerRevenue"}
AD_PERFORMANCE_METRICS = {"grossRevenue", "cpm", "adImpressions",
                          "monetizedPlaybacks", "playbackBasedCpm"}

# Possible Filters ------------------------------------------------------------

RESOURCE_FILTERS = {*RESOURCE_DIMENSIONS, "group"}
GEOGRAPHIC_FILTERS = {*GEOGRAPHIC_DIMENSIONS, "continent", "subContinent"}
AUDIENCE_RETENTION_FILTERS = {"audienceType"}
TRAFFIC_DETAIL_TYPES = {
   "ADVERTISING", "CAMPAIGN_CARD", "END_SCREEN", "EXT_URL", "HASHTAGS",
   "NOTIFICATION", "RELATED_VIDEO", "SOUND_PAGE", "SUBSCRIBER",
   "YT_CHANNEL", "YT_OTHER_PAGE", "YT_SEARCH", "VIDEO_REMIXES"
}
AUDIENCE_TYPES = {"ORGANIC", "AD_INSTREAM", "AD_INDISPLAY"}

# ----------------------------------------------------------------------------
# 1.Auth helpers — build an *AuthorizedSession* ready for the client
# ----------------------------------------------------------------------------



class AnalyticsClient:
    """Tiny Analytics client with retries, type‑coercion, and rate‑limit detection."""

    def __init__(self, session: AuthorizedSession):
        self.session = session
        self.base_url = "https://youtubeanalytics.googleapis.com/v2/reports"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.session.close()

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    @staticmethod
    def _to_dataframe(data: dict) -> pd.DataFrame:
        headers = data.get("columnHeaders", [])
        df = pd.DataFrame(data.get("rows", []), columns=[h["name"] for h in headers])

        dtype_map = {"INTEGER": "Int64", "FLOAT": "float", "DATE": "datetime64[ns]"}
        for h in headers:
            dtype = dtype_map.get(h.get("dataType"))
            if dtype:
                df[h["name"]] = df[h["name"]].astype(dtype, errors="ignore")

        for col in {"day", "month"} & set(df.columns):
            df[col] = pd.to_datetime(df[col], errors="ignore")

        return df

    @staticmethod
    def _resolve_max_results(time_period: str, start_date: str | date, end_date: str | date,
                             max_results: int | None) -> int:
        if max_results is not None:
            return max_results
        if time_period == "day":
            return (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1
        if time_period == "month":
            s = pd.to_datetime(start_date)
            e = pd.to_datetime(end_date)
            return (e.year - s.year) * 12 + (e.month - s.month) + 1
        raise ValueError("time_period must be 'day' or 'month' when max_results is omitted")

    @runtime_typecheck
    def reports_query(
            self,
            *,
            ids: str = "channel==MINE",
            metrics: str | Sequence[str] | None = None,
            dimensions: str | Sequence[str] | None = None,
            sort: str | None = None,
            max_results: int | None = 10,
            filters: str | None = None,
            start_date: str | date = "2000-01-01",
            end_date: str | date | None = None,
            currency: str | None = None,
            start_index: int | None = None,
            include_historical_channel_data: bool | None = None
    ) -> pd.DataFrame:
        """
        Send a single *YouTube Analytics* `reports.query` request and return the
        result as a pandas DataFrame`.

        Most other functions in this package are wrappers for this function with
        some arguments already populated. If none of the other prebuilt functions
        work for your use case, this is the function to turn to.

        Args:
            ids (str, optional): The `ids` request parameter.  Defaults to
                ``"channel==MINE"`` (i.e. the authorised user’s own channel).
            metrics (Iterable[str] | str, optional): Comma-separated string *or*
                iterable of metric names.  If None, defaults to
                ``("views", "estimatedMinutesWatched")``.  **Must** contain at
                least one metric.
            dimensions (Iterable[str] | str, optional): Comma-separated string or
                iterable of dimension names.
            sort (str, optional): Sort order.  If ``None`` we auto-sort descending
                on the *first* metric (e.g. ``"-views"``).
            max_results (int | None, optional):  ``None`` defaults to ``10``.
            filters (str, optional): Raw filter string, e.g.
                ``"country==US;video==abc123"``.
            start_date (str | date, optional): Start of reporting window
                (inclusive). If you pass a :class:`datetime.date`, we convert it to
                ISO-8601.
            end_date (str | date | None, optional): End of reporting window
                (inclusive).  ``None`` ⇒ today.
            currency (str, optional): 3-letter ISO code when requesting revenue
                metrics.
            start_index (int, optional): 1-based pagination index.
            include_historical_channel_data (bool, optional): When ``True``,
                include data from before the channel was linked to the current
                owner.

        Returns:
            pandas.DataFrame: A tidy dataframe whose columns mirror the API’s
            ``columnHeaders`` list, with dtypes coerced to sensible pandas types
            (``Int64``, ``float``, ``datetime64[ns]``).

        Raises:
            QuotaExceeded: If the API replies with HTTP 403 and a quota-related
                error code (``quotaExceeded``, ``userRateLimitExceeded`` …).
            YTAPIError: For any other failure (4xx, 5xx).

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.reports_query(
            ...     metrics=["views", "likes"],
            ...     dimensions=["day"],
            ...     start_date="2024-01-01",
            ...     end_date="2024-01-31",
            ...     max_results=None,
            ... )
            >>> df.head()
        """

        if metrics is None: metrics = ("views", "estimatedMinutesWatched")
        if end_date is None: end_date = date.today()

        if sort is None and metrics:
            first_metric = list(metrics)[0] if not isinstance(metrics, str) else metrics.split(",")[0]
            sort = f"-{first_metric}"

        params: dict[str, str] = {
            "ids": ids,
            "startDate": str(start_date),
            "endDate": str(end_date),
            "metrics": ",".join(list(metrics)) if not isinstance(metrics, str) else metrics,
        }
        if dimensions:
            params["dimensions"] = ",".join(list(dimensions))
        if filters:
            params["filters"] = filters
        if sort:
            params["sort"] = sort
        if max_results is not None:
            params["maxResults"] = str(max_results)
        if currency:
            params["currency"] = currency
        if start_index is not None:
            params["startIndex"] = str(start_index)
        if include_historical_channel_data is not None:
            params["includeHistoricalChannelData"] = str(include_historical_channel_data).lower()

        resp = self.session.get(self.base_url, params=params, timeout=60)
        raise_for_status(resp)

        return self._to_dataframe(resp.json())

    # -------------------------------------------------------------------------
    # Helper to fan‑out over many IDs
    # -------------------------------------------------------------------------
    def _per_id(
            self,
            *,
            id_kind: str,
            id_vals: str | Sequence[str],
            extra_filters: Sequence[str] = (),
            concurrency: int = 8,  # tweak to taste (≤10 QPS is safe)
            **kw,
    ) -> pd.DataFrame:
        """Fan-out over many IDs **in parallel** and concat the frames."""
        ids_list = [id_vals] if isinstance(id_vals, str) else list(id_vals)

        def _one(_id: str) -> pd.DataFrame:
            filters = ";".join([*extra_filters, f"{id_kind}=={_id}"]) if extra_filters else f"{id_kind}=={_id}"
            return self.reports_query(filters=filters, **kw)

        # Pool size = min(concurrency, #ids) so we don't spawn useless threads
        with ThreadPoolExecutor(max_workers=min(concurrency, len(ids_list))) as pool:
            # Submit tasks
            futures = [pool.submit(_one, _id) for _id in ids_list]

            # Collect as they finish (maintains retry/back-off behaviour inside reports_query)
            frames = [future.result() for future in as_completed(futures)]

        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    # -------------------------------------------------------------------------
    # Geographic Functions ----------------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_geography(self, video_ids: str | Sequence[str], *, geo_dim: str = "country",
            max_results: int = 200, **kw
    ) -> pd.DataFrame:
        """
        Returns video stats by geographical region (e.g. "country", "city", etc.)

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            geo_dim (str, optional): Geographic granularity—
                ``'country'`` (default), ``'province'``, ``'dma'``, or ``'city'``.
            max_results (int, optional): Maximum rows per API page. Defaults to 200.
            **kw: Extra keyword arguments forwarded intact to
                :py:meth:`reports_query` (e.g. ``start_date``, ``end_date``).

        Returns:
            pandas.DataFrame: A dataframe with one row per *video × geo_dim*.

        Raises:
            ValueError: If *geo_dim* is not one of the allowed values.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_geography(
            ...     ["dQw4w9WgXcQ", "HEXWRTEbj1I"],
            ...     geo_dim="dma",
            ...     start_date="2024-01-01",
            ...     end_date="2024-01-31",
            ... )
            >>> df.head()
        """
        if geo_dim not in GEOGRAPHIC_DIMENSIONS:
            _raise_invalid_argument("geo_dim", geo_dim, GEOGRAPHIC_DIMENSIONS)
        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            dimensions=("video", geo_dim),
            max_results=max_results,
            **kw,
        )

    @runtime_typecheck
    def channel_geography(self, *, geo_dim: str = "country",
                    max_results: int = 200, **kw
    ) -> pd.DataFrame:
        """
        Returns channel stats by geographical region (e.g. "country", "city", etc.)

        Args:
            geo_dim (str, optional): Geographic granularity—
                ``'country'`` (default), ``'province'``, ``'dma'``, or ``'city'``.
            max_results (int, optional): Maximum rows per API page. Defaults to 200.
            **kw: Extra keyword arguments forwarded intact to
                :py:meth:`reports_query` (e.g. ``start_date``, ``end_date``).

        Returns:
            pandas.DataFrame: A dataframe with one row per *geo_dim*.

        Raises:
            ValueError: If *geo_dim* is not one of the allowed values.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_geography(
            ...     geo_dim="dma",
            ...     start_date="2024-01-01",
            ...     end_date="2024-01-31",
            ... )
            >>> df.head()
        """
        if geo_dim not in GEOGRAPHIC_DIMENSIONS:
            _raise_invalid_argument("geo_dim", geo_dim, GEOGRAPHIC_DIMENSIONS)
        return self.reports_query(
            dimensions=(geo_dim,),
            max_results=max_results,
            **kw,
        )

    # -------------------------------------------------------------------------
    # Playback Location Functions ---------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_playback_location(self, video_ids: str | Sequence[str], *, detail: bool = False,
                                max_results: int = 200, **kw
    ) -> pd.DataFrame:
        """
        Return where viewers watched each video (YouTube, embedded players, etc.).

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            detail (bool, optional):
                * **False** (default) – group results by high-level
                  ``insightPlaybackLocationType`` (e.g. *EMBEDDED*, *YOUTUBE*).
                * **True** – drill into ``insightPlaybackLocationDetail``; the API
                  automatically filters to ``insightPlaybackLocationType==EMBEDDED``
                  and caps `max_results` at 25.
            max_results (int, optional): Requested page size. Ignored
                when *detail* is ``True`` because the API hard-caps it.
            **kw: Extra keyword arguments forwarded verbatim to
                :py:meth:`reports_query` (e.g. ``start_date``, ``end_date``).

        Returns:
            pandas.DataFrame: One row per *video × playback-location*

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_playback_location(
            ...     ["dQw4w9WgXcQ"],
            ...     detail=True,
            ...     start_date="2024-01-01",
            ...     end_date="2024-03-31",
            ... )
            >>> df.head()
        """
        dim = "insightPlaybackLocationDetail" if detail else "insightPlaybackLocationType"
        extras = ["insightPlaybackLocationType==EMBEDDED"] if detail else []
        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            extra_filters=extras,
            dimensions=(dim,),
            max_results=max_results if not detail else 25,
            **kw,
        )

    @runtime_typecheck
    def channel_playback_location(self, *, detail: bool = False,
                                  max_results: int = 200, **kw
    ) -> pd.DataFrame:
        """
        Return where viewers watched channel videos (YouTube, embedded players, etc.).

        Args:
            detail (bool, optional):
                * **False** (default) – group results by high-level
                  ``insightPlaybackLocationType`` (e.g. *EMBEDDED*, *YOUTUBE*).
                * **True** – drill into ``insightPlaybackLocationDetail``; the API
                  automatically filters to ``insightPlaybackLocationType==EMBEDDED``
                  and caps `max_results` at 25.
            max_results (int, optional): Requested page size. Ignored
                when *detail* is ``True`` because the API hard-caps it.
            **kw: Extra keyword arguments forwarded verbatim to
                :py:meth:`reports_query` (e.g. ``start_date``, ``end_date``).

        Returns:
            pandas.DataFrame: One row per *playback-location*.

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_playback_location(
            ...     detail=True,
            ...     start_date="2024-01-01",
            ...     end_date="2024-03-31",
            ... )
            >>> df.head()
        """
        dim = "insightPlaybackLocationDetail" if detail else "insightPlaybackLocationType"
        filters = "insightPlaybackLocationType==EMBEDDED" if detail else None
        return self.reports_query(
            dimensions=(dim,),
            filters=filters,
            max_results=max_results if not detail else 25,
            **kw,
        )

    # -------------------------------------------------------------------------
    # Playback Details Functions ----------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_playback_details(self, video_ids: str | Sequence[str], *,
                               detail: str = "liveOrOnDemand", **kw
    ) -> pd.DataFrame:
        """
        Break down each video by a playback-detail dimension (live vs. VOD, etc.).

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            detail (str, optional): Dimension to split by—must be one of
                ``'creatorContentType'``, ``'liveOrOnDemand'``,
                ``'subscribedStatus'``, ``'youtubeProduct'``.
                Defaults to ``'liveOrOnDemand'``.
            **kw: Extra keyword arguments forwarded verbatim to
                :py:meth:`reports_query` (e.g. ``start_date``, ``metrics``).

        Returns:
            pandas.DataFrame: One row per *video × detail*.

        Raises:
            ValueError: If *detail* is not one of the allowed literals.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_playback_details(
            ...     ["dQw4w9WgXcQ"],
            ...     detail="subscribedStatus",
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        if detail not in PLAYBACK_DETAIL_DIMENSIONS:
            _raise_invalid_argument("detail", detail, PLAYBACK_DETAIL_DIMENSIONS)
        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            dimensions=(detail,),
            **kw,
        )

    @runtime_typecheck
    def channel_playback_details(self, *, detail: str = "liveOrOnDemand", **kw,
    ) -> pd.DataFrame:
        """
        Break down channel stats by a playback-detail dimension (live vs. VOD, etc.).

        Args:
            detail (str, optional): Dimension to split by—must be one of
                ``'creatorContentType'``, ``'liveOrOnDemand'``,
                ``'subscribedStatus'``, ``'youtubeProduct'``.
                Defaults to ``'liveOrOnDemand'``.
            **kw: Extra keyword arguments forwarded verbatim to
                :py:meth:`reports_query` (e.g. ``start_date``, ``metrics``).

        Returns:
            pandas.DataFrame: One row per *detail* value, with the metrics
            requested (default ``views`` & ``estimatedMinutesWatched``).

        Raises:
            ValueError: If *detail* is not one of the allowed literals.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_playback_details(
            ...     detail="subscribedStatus",
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        if detail not in PLAYBACK_DETAIL_DIMENSIONS:
            _raise_invalid_argument("detail", detail, PLAYBACK_DETAIL_DIMENSIONS)
        return self.reports_query(
            dimensions=(detail,),
            **kw
        )

    # -------------------------------------------------------------------------
    # Device Functions --------------------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_devices(self, video_ids: str | Sequence[str], *,
                      device_info: str | Sequence[str] = "deviceType", **kw,
    ) -> pd.DataFrame:

        """
        Break down each video by viewers’ device characteristics.

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            device_info (str | Sequence[str], optional):
                Either a single literal or an iterable drawn from:
                - ``"deviceType"`` – desktop, mobile, tablet, TV, etc. *(default)*
                - ``"operatingSystem"`` – iOS, Android, Windows, macOS, etc.

                You may pass both to get a multi-dimension report, e.g.
                ``device_info=("deviceType", "operatingSystem")``.
            **kw: Additional keyword arguments forwarded unchanged to
                :py:meth:`reports_query` (such as ``start_date``, ``metrics``).

        Returns:
            pandas.DataFrame: One row per *video × device combo* containing the
            requested metrics (defaults to ``views`` and
            ``estimatedMinutesWatched`` if not overridden).

        Raises:
            ValueError: If *device_info* contains anything outside
                ``{"deviceType", "operatingSystem"}``.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_devices(
            ...     ["dQw4w9WgXcQ", "HEXWRTEbj1I"],
            ...     device_info=("deviceType", "operatingSystem"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        dims = _string_to_tuple(device_info)
        if not set(dims).issubset(DEVICE_DIMENSIONS):
            _raise_invalid_argument("device_info", device_info,
                                         DEVICE_DIMENSIONS)
        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            dimensions=dims,
            **kw,
        )

    @runtime_typecheck
    def channel_devices(self, *,
                        device_info: str | Sequence[str] = "deviceType", **kw,
    ) -> pd.DataFrame:
        """
        Break down channel stats by viewers’ device characteristics.

        Args:
            device_info (str | Sequence[str], optional):
                Either a single literal or an iterable drawn from:
                - ``"deviceType"`` – desktop, mobile, tablet, TV, etc. *(default)*
                - ``"operatingSystem"`` – iOS, Android, Windows, macOS, etc.

                You may pass both to get a multi-dimension report, e.g.
                ``device_info=("deviceType", "operatingSystem")``.
            **kw: Additional keyword arguments forwarded unchanged to
                :py:meth:`reports_query` (such as ``start_date``, ``metrics``).

        Returns:
            pandas.DataFrame: One row per device characteristic.

        Raises:
            ValueError: If *device_info* contains anything outside
                ``{"deviceType", "operatingSystem"}``.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_devices(
            ...     device_info=("deviceType", "operatingSystem"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        dims = _string_to_tuple(device_info)
        if not set(dims).issubset(DEVICE_DIMENSIONS):
            _raise_invalid_argument("device_info", device_info,
                                         DEVICE_DIMENSIONS)
        return self.reports_query(dimensions=dims, **kw)

    # -------------------------------------------------------------------------
    # Demographic Functions ---------------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_demographics(self, video_ids: str | Sequence[str], *,
            demographic: str | Sequence[str] = "ageGroup", **kw,
    ) -> pd.DataFrame:
        """
        Break down each video’s audience by age and/or gender.

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            demographic (str | Sequence[str], optional):
                A single literal or an iterable chosen from:
                - ``"ageGroup"`` – 13-17, 18-24, …, 65-plus *(default)*
                - ``"gender"`` – *male*, *female*, *user-specified*
                Pass both to obtain a multi-dimension report, e.g.
                ``demographic=("ageGroup", "gender")``.
            **kw: Extra keyword arguments forwarded verbatim to
                :py:meth:`reports_query` (for example ``start_date``,
                ``end_date``, or a custom ``metrics`` tuple).

        Returns:
            pandas.DataFrame: One row per *video × demographic combo* with the
            requested metrics (defaults to ``views`` and
            ``estimatedMinutesWatched`` if not overridden).

        Raises:
            ValueError: If *demographic* contains anything outside
                ``{"ageGroup", "gender"}``.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_demographics(
            ...     ["dQw4w9WgXcQ", "HEXWRTEbj1I"],
            ...     demographic=("ageGroup", "gender"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        dims = _string_to_tuple(demographic)
        if not set(dims).issubset(DEMOGRAPHIC_DIMENSIONS):
            _raise_invalid_argument("demographic", demographic,
                                         DEMOGRAPHIC_DIMENSIONS)
        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            dimensions=dims,
            **kw,
        )

    @runtime_typecheck
    def channel_demographics(self, *,
            demographic: str | Sequence[str] = "ageGroup", **kw,
    ) -> pd.DataFrame:
        """
        Break down channel's audience by age and/or gender.

        Args:
            demographic (str | Sequence[str], optional):
                A single literal or an iterable chosen from:
                - ``"ageGroup"`` – 13-17, 18-24, …, 65-plus *(default)*
                - ``"gender"`` – *male*, *female*, *user-specified*
                Pass both to obtain a multi-dimension report, e.g.
                ``demographic=("ageGroup", "gender")``.
            **kw: Extra keyword arguments forwarded verbatim to
                :py:meth:`reports_query` (for example ``start_date``,
                ``end_date``, or a custom ``metrics`` tuple).

        Returns:
            pandas.DataFrame: One row per demographic combo.

        Raises:
            ValueError: If *demographic* contains anything outside
                ``{"ageGroup", "gender"}``.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_demographics(
            ...     demographic=("ageGroup", "gender"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        dims = _string_to_tuple(demographic)
        if not set(dims).issubset(DEMOGRAPHIC_DIMENSIONS):
            _raise_invalid_argument("demographic", demographic,
                                         DEMOGRAPHIC_DIMENSIONS)
        return self.reports_query(dimensions=dims, **kw)


    # -------------------------------------------------------------------------
    # Statistics Functions ----------------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_stats(self, video_ids: str | Sequence[str], **kw) -> pd.DataFrame:
        """
        Get stats for one or more videos.

        This is a generic video stats helper: whatever metrics/dimensions you pass
        through ``**kw`` pass to :py:meth:`reports_query`, which means
        you can ask for any Analytics combo without creating a new wrapper.

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            **kw:  Keyword arguments forwarded verbatim to
                :py:meth:`reports_query` — for example ``metrics``,
                ``dimensions``, ``start_date``, ``end_date``, ``filters``, etc.

        Returns:
            pandas.DataFrame: One row per *video × requested dimension(s)* with the
            metrics you asked for (defaults to ``views`` and
            ``estimatedMinutesWatched``).

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_stats(
            ...     ["dQw4w9WgXcQ", "HEXWRTEbj1I"],
            ...     metrics=("views", "likes", "comments"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            **kw,
        )

    @runtime_typecheck
    def channel_stats(self, **kw) -> pd.DataFrame:
        """
        Get stats for a channel.

        This is a generic channel stats helper: whatever metrics/dimensions you pass
        through ``**kw`` pass to :py:meth:`reports_query`, which means
        you can ask for any Analytics combo without creating a new wrapper.

        Args:
            **kw:  Keyword arguments forwarded verbatim to
                :py:meth:`reports_query` — for example ``metrics``,
                ``dimensions``, ``start_date``, ``end_date``, ``filters``, etc.

        Returns:
            pandas.DataFrame: One row per requested *dimension(s)*.

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_stats(
            ...     metrics=("views", "likes", "comments"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        return self.reports_query(
            **kw
        )

    # -------------------------------------------------------------------------
    # Sharing Services Functions ----------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_sharing_services(self, video_ids: str | Sequence[str], **kw) -> pd.DataFrame:
        """
        Show which social / messaging platforms drove shares for each video.

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *video × sharingService*.

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_sharing_services(
            ...     ["dQw4w9WgXcQ", "HEXWRTEbj1I"],
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """

        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            dimensions=("sharingService",),
            metrics=("shares",),
            **kw,
        )

    @runtime_typecheck
    def channel_sharing_services(self, **kw) -> pd.DataFrame:
        """
        Show which social / messaging platforms drove shares to the channel.

        Args:
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *sharingService*.

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
               :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_sharing_services(
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        return self.reports_query(
            dimensions=("sharingService",),
            metrics=("shares",),
            **kw
        )

    # -------------------------------------------------------------------------
    # Time Period Functions ---------------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_time_period(self, video_ids: str | Sequence[str], *,
                          time_period: str = "month", start_date: str | date,
                          end_date: str | date, max_results: int | None = None,
                          **kw,
    ) -> pd.DataFrame:
        """
        Summarise video performance by calendar **day** or **month**.

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            time_period (str, optional): Reporting grain—either ``"day"`` or
                ``"month"``. Defaults to ``"month"``.
            start_date (str | datetime.date): ISO ``YYYY-MM-DD`` or date object
                marking the start of the reporting window (inclusive).
            end_date (str | datetime.date): ISO ``YYYY-MM-DD`` or date object
                marking the end of the reporting window (inclusive).
            max_results (int | None, optional): Number of rows to return.
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *video × {day | month}* with the metrics
            requested (defaults to ``views`` and ``estimatedMinutesWatched``).

        Raises:
            ValueError: If *time_period* is not ``"day"`` or ``"month"``.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_time_period(
            ...     ["dQw4w9WgXcQ", "HEXWRTEbj1I"],
            ...     time_period="day",
            ...     start_date="2024-01-01",
            ...     end_date="2024-01-31",
            ... )
            >>> df.head()
        """
        if time_period not in TIME_PERIOD_DIMENSIONS:
            _raise_invalid_argument("time_period", time_period,
                                         TIME_PERIOD_DIMENSIONS)
        resolved_max = self._resolve_max_results(time_period, start_date, end_date, max_results)
        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            dimensions=(time_period,),
            start_date=start_date,
            end_date=end_date,
            sort=time_period,
            max_results=resolved_max,
            **kw,
        )

    @runtime_typecheck
    def channel_time_period(self, *, time_period: str = "month",
                            start_date: str | date, end_date: str | date,
                            max_results: int | None = None, **kw,
    ) -> pd.DataFrame:
        """
        Summarise channel performance by calendar **day** or **month**.

        Args:
            time_period (str, optional): Reporting grain—either ``"day"`` or
                ``"month"``. Defaults to ``"month"``.
            start_date (str | datetime.date): ISO ``YYYY-MM-DD`` or date object
                marking the start of the reporting window (inclusive).
            end_date (str | datetime.date): ISO ``YYYY-MM-DD`` or date object
                marking the end of the reporting window (inclusive).
            max_results (int | None, optional): Number of rows to return.
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per  *{day | month}*.

        Raises:
            ValueError: If *time_period* is not ``"day"`` or ``"month"``.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_time_period(
            ...     time_period="day",
            ...     start_date="2024-01-01",
            ...     end_date="2024-01-31",
            ... )
            >>> df.head()
        """
        if time_period not in TIME_PERIOD_DIMENSIONS:
            _raise_invalid_argument("time_period", time_period,
                                         TIME_PERIOD_DIMENSIONS)
        resolved_max = self._resolve_max_results(time_period, start_date, end_date, max_results)
        return self.reports_query(
            dimensions=(time_period,),
            start_date=start_date,
            end_date=end_date,
            sort=time_period,
            max_results=resolved_max,
            **kw,
        )

    # -------------------------------------------------------------------------
    # Top Videos Functions ----------------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def playlist_top_videos(self, playlist_ids: str | Sequence[str], **kw) -> pd.DataFrame:
        """
        Return the top-performing videos within one or more playlists.

        Args:
            playlist_ids (str | Sequence[str]): One or more YouTube playlist IDs.
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *video × playlist* , with whatever metrics
            requested (defaults to ``views`` and ``estimatedMinutesWatched``).

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.playlist_top_videos(
            ...     ["PL9tY0BWXOZFtQ-GG8X2E8oia-MfeLeGKv"],
            ...     metrics=("views", "likes", "comments"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        return self._per_id(
            id_kind="playlist",
            id_vals=playlist_ids,
            dimensions=("video",),
            extra_filters=("isCurated==1",),
            **kw,
        )

    @runtime_typecheck
    def channel_top_videos(self, **kw) -> pd.DataFrame:
        """
        Return the top-performing videos in the channel.

        Args:
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *video*.

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_top_videos(
            ...     metrics=("views", "likes", "comments"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        return self.reports_query(
            dimensions=("video",),
            **kw
        )

    # -------------------------------------------------------------------------
    # Traffic Sources Functions -----------------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_traffic_sources(self, video_ids: str | Sequence[str], *,
                              detail: str | None = None, **kw
    ) -> pd.DataFrame:
        """
        Break down traffic sources for one or more videos.

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            detail (str | None, optional):
                * ``None`` (default) – group rows by high-level
                  **insightTrafficSourceType** (e.g. *YT_SEARCH*, *RELATED_VIDEO*).
                * Any literal in
                  ``{"ADVERTISING", "CAMPAIGN_CARD", "END_SCREEN", "EXT_URL",
                  "HASHTAGS", "NOTIFICATION", "RELATED_VIDEO", "SOUND_PAGE",
                  "SUBSCRIBER", "YT_CHANNEL", "YT_OTHER_PAGE", "YT_SEARCH",
                  "VIDEO_REMIXES"}`` – drill into **insightTrafficSourceDetail** for that specific type.

            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame:
            * If *detail* is ``None`` – one row per *video ×
              insightTrafficSourceType*.
            * If *detail* is provided – one row per *video ×
              insightTrafficSourceDetail*.

            Metrics default to ``views`` and ``estimatedMinutesWatched`` unless
            overridden via ``**kw``.

        Raises:
            ValueError: If *detail* is not ``None`` and not in the allowed literal
                set shown above.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_traffic_sources(
            ...     ["dQw4w9WgXcQ"],
            ...     detail="YT_SEARCH",
            ...     start_date="2024-01-01",
            ...     end_date="2024-03-31",
            ... )
            >>> df.head()
        """
        if detail is not None and detail not in TRAFFIC_DETAIL_TYPES:
            _raise_invalid_argument("detail", detail,
                                         TRAFFIC_DETAIL_TYPES)
        dim = "insightTrafficSourceDetail" if detail is not None \
            else "insightTrafficSourceType"
        extras = (f"insightTrafficSourceType=={detail}",) if detail is not None \
            else ()

        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            extra_filters=extras,
            dimensions=dim,
            max_results= 25,
            **kw,
        )

    @runtime_typecheck
    def channel_traffic_sources(self, *, detail: str | None = None, **kw
    ) -> pd.DataFrame:
        """
        Break down traffic sources for the channel.

        Args:
            detail (str | None, optional):
                * ``None`` (default) – group rows by high-level
                  **insightTrafficSourceType** (e.g. *YT_SEARCH*, *RELATED_VIDEO*).
                * Any literal in
                  ``{"ADVERTISING", "CAMPAIGN_CARD", "END_SCREEN", "EXT_URL",
                  "HASHTAGS", "NOTIFICATION", "RELATED_VIDEO", "SOUND_PAGE",
                  "SUBSCRIBER", "YT_CHANNEL", "YT_OTHER_PAGE", "YT_SEARCH",
                  "VIDEO_REMIXES"}`` – drill into
                  **insightTrafficSourceDetail** for that specific type.
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame:
            * If *detail* is ``None`` – one row per *insightTrafficSourceType*.
            * If *detail* is provided – one row per *insightTrafficSourceDetail*.

        Raises:
            ValueError: If *detail* is not ``None`` and not in the allowed literal
                set shown above.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_traffic_sources(
            ...     detail="YT_SEARCH",
            ...     start_date="2024-01-01",
            ...     end_date="2024-03-31",
            ... )
            >>> df.head()
        """
        if detail is not None and detail not in TRAFFIC_DETAIL_TYPES:
            _raise_invalid_argument("detail", detail,
                                         TRAFFIC_DETAIL_TYPES)
        dim = "insightTrafficSourceDetail" if detail \
            else "insightTrafficSourceType"
        filters = f"insightTrafficSourceType=={detail}" if detail \
            else None

        return self.reports_query(
            dimensions=(dim,),
            filters=filters,
            max_results=25,
            **kw,
        )

    # -------------------------------------------------------------------------
    # Audience retention (videos only) ----------------------------------------
    # -------------------------------------------------------------------------
    @runtime_typecheck
    def video_audience_retention(self, video_ids: str | Sequence[str], *,
                                 audience_type: str | None = None, **kw
    ) -> pd.DataFrame:
        """
        Chart how well viewers stick around for each video (audience‐retention).

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            audience_type (str | None, optional):
                Filter the report by viewer origin:

                - ``"ORGANIC"`` – regular, unpaid views
                - ``"AD_INSTREAM"`` – pre-roll / mid-roll ad views
                - ``"AD_INDISPLAY"`` – video discovery ads
                - ``None`` (default) – all audiences combined

                Any other literal triggers a tidy bullet-listed `ValueError`.
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *video × elapsedVideoTimeRatio* bucket,
            with the ``audienceWatchRatio`` metric.

        Raises:
            ValueError: If *audience_type* is not ``None`` and not in
                ``{"ORGANIC", "AD_INSTREAM", "AD_INDISPLAY"}``.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_audience_retention(
            ...     ["dQw4w9WgXcQ"],
            ...     audience_type="ORGANIC",
            ...     start_date="2024-01-01",
            ...     end_date="2024-03-31",
            ... )
            >>> df.head()
        """
        if audience_type is not None and audience_type not in AUDIENCE_TYPES:
            _raise_invalid_argument("audience_type", audience_type,
                                         AUDIENCE_TYPES)
        extras = (f"audienceType=={audience_type}",) if audience_type is not None \
            else ()
        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            extra_filters=extras,
            dimensions=("elapsedVideoTimeRatio",),
            metrics = ("audienceWatchRatio",),
            **kw,
        )

    # ------------------------------------------------------------------
    # Live‑streaming position (videos only) -----------------------------
    # ------------------------------------------------------------------
    @runtime_typecheck
    def video_live_position(self, video_ids: str | Sequence[str], *,
                            metrics: str | Sequence[str] = "peakConcurrentViewers",
                            **kw
                            ) -> pd.DataFrame:
        """
        Fetch live-stream performance by in-broadcast position.

        The API’s **liveStreamPosition** dimension buckets data by how far a viewer
        was into the stream when they joined (0–10 %, 10–25 %, etc.).

        Args:
            video_ids (str | Sequence[str]): One or more YouTube video IDs.
            metrics (str | Sequence[str], optional): Metric name or collection
                drawn from:
                - ``"averageConcurrentViewers"``
                - ``"peakConcurrentViewers"`` *(default)*
                Pass a list/tuple when you need both.
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *video × liveStreamPosition bucket* with
            the requested metric columns.

        Raises:
            ValueError: If *metrics* is empty or contains anything outside
                ``{"averageConcurrentViewers", "peakConcurrentViewers"}``.
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.

        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.video_live_position(
            ...     ["dQw4w9WgXcQ"],
            ...     metrics=("averageConcurrentViewers", "peakConcurrentViewers"),
            ...     start_date="2024-05-01",
            ...     end_date="2024-05-31",
            ... )
            >>> df.head()
        """
        mets = _string_to_tuple(metrics)
        if not mets or not set(mets).issubset(LIVESTREAM_METRICS):
            _raise_invalid_argument("metrics", metrics,
                                         LIVESTREAM_METRICS)

        return self._per_id(
            id_kind="video",
            id_vals=video_ids,
            dimensions=("liveStreamPosition",),
            metrics=mets,
            **kw,
        )

    # ------------------------------------------------------------------
    # Membership cancellation (channel only) ---------------------------
    # ------------------------------------------------------------------
    @runtime_typecheck
    def channel_membership_cancellation(self, **kw) -> pd.DataFrame:
        """
        Analyse **why** paying members cancel their channel memberships.

        The report groups rows by ``membershipCancellationSurveyReason`` and
        returns the metric ``membershipsCancellationSurveyResponses`` (count of
        answers per reason).  Use this to spot the top churn drivers—price,
        content cadence, “just browsing,” etc.

        Args:
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *membershipCancellationSurveyReason*
            with the ``membershipsCancellationSurveyResponses`` metric.

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_membership_cancellation(
            ...     start_date="2024-01-01",
            ...     end_date="2024-06-30",
            ... )
            >>> df.head()
        """
        return self.reports_query(
            dimensions=("membershipCancellationSurveyReason",),
            metrics ="membershipsCancellationSurveyResponses", **kw
        )

    # ------------------------------------------------------------------
    # Ad‑performance ----------------------------------------------------
    # ------------------------------------------------------------------
    @runtime_typecheck
    def channel_ad_performance(self, **kw) -> pd.DataFrame:
        """
        Inspect ad-revenue performance for the authenticated channel.

        The helper queries the Analytics API with
        ``dimensions=("adType")``—*display*, *bumper*, *skippable in-stream*, etc.—
        and defaults to the ``adRate`` metric (revenue per 1 000 monetized
        playbacks).  Override ``metrics`` in ``**kw`` if you need additional
        figures such as ``grossRevenue`` or ``cpm``.

        Args:
            **kw: Keyword arguments forwarded unchanged to
                :py:meth:`reports_query` – e.g. ``start_date``, ``end_date``,
                or a custom ``filters`` string.

        Returns:
            pandas.DataFrame: One row per *adType* with the requested metric
            columns.

        Raises:
            QuotaExceeded / AnalyticsError: Propagated from
                :py:meth:`reports_query`.


        Example:
            >>> yt = AnalyticsClient(creds)
            >>> df = yt.channel_ad_performance(
            ...     metrics=("adRate", "grossRevenue"),
            ...     start_date="2024-01-01",
            ...     end_date="2024-12-31",
            ... )
            >>> df.head()
        """
        return self.reports_query(dimensions=("adType",), metrics=("adRate",), **kw)
