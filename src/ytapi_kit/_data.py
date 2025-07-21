from __future__ import annotations

import re
from typing import Mapping, MutableMapping, Sequence, Iterable
import pandas as pd
from datetime import datetime, date
import itertools

from ._errors import raise_for_status
from ._util import runtime_typecheck, _validate_enum, _prune_none, _paged_list

__all__ = ["DataClient"]

class DataClient:
    """High-level wrapper around the **YouTube Data API v3**.

    The client offers:

    * Wrappers for every “**.list**” GET endpoint
      (``list_videos``, ``list_playlists``, ``list_search`` …).
    * Automatic pagination, ID-chunking, and JSON→DataFrame conversion.
    * Convenient aggregators such as
      :py:meth:`channel_playlists`, :py:meth:`playlist_videos`,
      :py:meth:`channel_videos`, and :py:meth:`video_metadata`.

    Args:
        session (google.auth.transport.requests.AuthorizedSession):
            Pre-authenticated HTTP session.  The client never refreshes or
            mutates the credentials object; bring your own refresh logic if
            needed.
        base_url (str, optional):
            API root to use instead of the default
            ``"https://youtube.googleapis.com/youtube/v3/"``.

    Attributes:
        session (AuthorizedSession):
            The underlying HTTP session.  Closed automatically when the client
            is used as a context manager *and* the session was created inside
            the client.
        base_url (str):
            Root prefix for every endpoint path.

    Methods
    -------
    Core list wrappers
        ``list_activities``, ``list_captions``, ``list_channels``,
        ``list_channel_sections``, ``list_comments``, ``list_comment_threads``,
        ``list_i18n_languages``, ``list_i18n_regions``, ``list_members``,
        ``list_membership_levels``, ``list_playlist_images``,
        ``list_playlist_items``, ``list_playlists``, ``list_search``,
        ``list_subscriptions``, ``list_video_abuse_report_reasons``,
        ``list_video_categories``, ``list_videos``.
    High-level helpers
        ``channel_playlists``, ``playlist_videos``, ``channel_videos``,
        ``video_metadata``.

    Raises:
        google.auth.exceptions.TransportError:
            Propagated from the underlying session if the network fails.
        ytapi_kit.RateLimitError:
            Raised when the API returns *429 Too Many Requests* or a quota
            error; see :pyfunc:`ytapi_kit._errors.raise_for_status`.
        ValueError, TypeError:
            Argument-validation errors surfaced verbatim from individual
            endpoint helpers.

    Examples:
         dc = DataClient(AuthorizedSession(creds))
         playlists = dc.channel_playlists(mine=True)
         playlists[["id", "snippet.title"]].head()

    """

    def __init__(self, session, base_url: str = "https://www.googleapis.com/youtube/v3"):
        self.session = session
        self.base_url = base_url

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    @staticmethod
    def _iso(dt: datetime | date | str) -> str:
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

    @staticmethod
    def _chunk(iterable: Iterable[str], size: int = 50):
        """Yield successive `size`-length chunks from *iterable*."""
        it = iter(iterable)
        while chunk := list(itertools.islice(it, size)):
            yield chunk

    def _data_request(
            self,
            method: str,
            path: str,
            params: MutableMapping[str, object] | None = None,
            *,
            json_data: Mapping | None = None,
            files: Mapping[str, tuple] | None = None,
            stream: bool = False,
    ):
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
        payload = self._data_request("GET", f"/{resource}", dict(params))
        return self._to_dataframe(payload.get("items", [])), payload.get("nextPageToken")

    @runtime_typecheck
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
        """Wrapper for the **activities.list** endpoint.

        Args:
            channel_id (str | None):
                ID of a specific channel. **Exactly one** of *channel_id* or
                *mine* must be supplied.
            mine (bool | None):
                Set to ``True`` to fetch activities for the authenticated user’s
                own channel.
            part (str | Sequence[str]):
                **Required.** One or more of:
                - ``"contentDetails"``
                - ``"id"``
                - ``"snippet"``
            published_after (datetime | date | str | None):
                Earliest date/time (inclusive) the activity occurred.
            published_before (datetime | date | str | None):
                Latest date/time (exclusive) the activity occurred.
            region_code (str | None):
                Two-letter ISO 3166-1 alpha-2 region code used to filter results.
            page_token (str | None):
                Token that identifies the results page to return.
            max_results (int | None):
                Maximum items per page (1–50). Larger result sets require
                pagination via *page_token*.

        Returns:
        tuple[pandas.DataFrame, str | None]:
            - DataFrame containing the activities.
            - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If both or neither of *channel_id* and *mine* are supplied.

        References:
            https://developers.google.com/youtube/v3/docs/activities/list
        """
        if sum(map(bool, (channel_id, mine))) != 1:
            raise ValueError("Supply exactly one of channel_id, or mine=True.")

        _ACTIVITY_PARTS_ALLOWED = {"contentDetails", "id", "snippet"}
        parts = _validate_enum("part", part, _ACTIVITY_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "channelId": channel_id,
            "mine": str(mine).lower() if mine else None,
            "publishedAfter": self._iso(published_after) if published_after else None,
            "publishedBefore": self._iso(published_before) if published_before else None,
            "regionCode": region_code,
            "pageToken": page_token,
            "maxResults": max_results,
        })

        return self._list_helper("activities", params=params)

    @runtime_typecheck
    def list_captions(
            self,
            *,
            part: str | Sequence[str] = ("id", "snippet"),
            video_id: str,
            caption_id: str | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for the **captions.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** Set to one or more of the following values:
                    - "id"
                    - "snippet
            video_id (str):
                **Required.** ID of specific video
            caption_id (str | None):
                ID of specific caption resource to be retrieved
            on_behalf_of_content_owner (str | None):
                ID of content owner

        Returns:
            pandas.Dataframe:
                Dataframe containing the captions.

        Raises:
            ValueError: If part is not an allowed value.
            TypeError: If any arguments passed are invalid.

        References:
            https://developers.google.com/youtube/v3/docs/captions/list
        """

        _CAPTION_PARTS_ALLOWED = {"id", "snippet"}
        parts = _validate_enum("part", part, _CAPTION_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "videoId": video_id,
            "id": caption_id,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
        })

        df, _ = self._list_helper("captions", params=params)
        return df

    @runtime_typecheck
    def list_channels(
            self,
            *,
            part: str | Sequence[str] = ("contentDetails", "snippet"),
            for_handle: str | None = None,
            for_username: str | None = None,
            channel_id: str | None = None,
            managed_by_me: bool | None = None,
            mine: bool | None = None,
            hl: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            page_token: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for the **channels.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** Set to one or more of the following values:
                    - "auditDetails"
                    - "brandingSettings"
                    - "contentDetails"
                    - "contentOwnerDetails"
                     - "id"
                     - "localizations"
                     - "snippet"
                     - "statistics"
                     - "status"
                     - "topicDetails"
            for_handle (str | None):
                Handle of specific channel. **Exactly one** of *for_handle* , *for_username*,
                *id*, *managed_by_me*, or *mine* must be supplied.
            for_username (str | None):
                Username of specific channel.
            channel_id (str | None):
                ID of specific channel.
            managed_by_me (bool | None):
                Set to ``True`` to fetch channels managed by the user.
            mine (bool | None):
                Set to ``True`` to fetch channels owned by the user.
            hl (str | None):
                A specified language that the YouTube website supports.
            max_results (int | None):
                Maximum items per page (1–50). Larger result sets require
                pagination via *page_token*.
            on_behalf_of_content_owner (str | None):
                ID of content owner
            page_token (str | None):
                Token that identifies the results page to return.

        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the activities.
                 - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If both or neither of *channel_id* and *mine* are supplied.

        References:
             https://developers.google.com/youtube/v3/docs/channels/list
        """
        if sum(map(bool, (for_handle, for_username, channel_id, managed_by_me, mine))) != 1:
            raise ValueError("Supply exactly one of **for_handle**, **for_username**, **channel_id**, **managed_by_me**, **mine**.")

        _CHANNEL_PARTS_ALLOWED = {"auditDetails", "brandingSettings", "contentDetails",
                                  "contentOwnerDetails", "id", "localizations", "snippet",
                                  "statistics", "status", "topicDetails"}
        parts = _validate_enum("part", part, _CHANNEL_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "forHandle": for_handle,
            "forUsername": for_username,
            "id": channel_id,
            "managedByMe": managed_by_me,
            "mine": str(mine).lower() if mine else None,
            "hl": hl,
            "maxResults": max_results,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "pageToken": page_token,
        })

        return self._list_helper("channels", params=params)

    @runtime_typecheck
    def list_channel_sections(
            self,
            *,
            part: str | Sequence[str] = ("contentDetails", "snippet"),
            channel_id: str | None = None,
            channel_section_id: str | None = None,
            mine: bool | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for the **channelSections.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** Set to one or more of the following values:
                    - "contentDetails"
                     - "id"
                     - "snippet"
            channel_id (str | None):
                **Required.** ID of specific channel. **Exactly one** of *channel_id* ,
                *id*, or *mine* must be supplied.
            channel_section_id (str | None):
                ID of specific `channelSection` resource.
            mine (bool | None):
                Set to ``True`` to fetch channels owned by the user.
            on_behalf_of_content_owner (str | None):
                ID of content owner

        Returns:
            pandas.DataFrame:
                - DataFrame containing the channel sections.

        Raises:
            ValueError: If not exactly one of *channel_id*, *channel_section_id*, or *mine* are supplied.

        References:
             https://developers.google.com/youtube/v3/docs/channels/list
        """
        if sum(map(bool, (channel_id, channel_section_id, mine))) != 1:
            raise ValueError("Supply exactly one of channel_id, channel_section_id, or mine=True.")

        _CHANNEL_SECTION_PARTS_ALLOWED = {"contentDetails", "id", "snippet"}
        parts = _validate_enum("part", part, _CHANNEL_SECTION_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "channelId": channel_id,
            "id": channel_section_id,
            "mine": str(mine).lower() if mine else None,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
        })

        df, _ = self._list_helper("channelSections", params=params)
        return df

    @runtime_typecheck
    def list_comments(
            self,
            *,
            part: str | Sequence[str] = ("contentDetails", "snippet"),
            comment_id: str | None = None,
            parent_id: str | None = None,
            max_results: int | None = None,
            page_token: str | None = None,
            text_format: str | None = None,

    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for the **comments.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** Set to one or more of the following values:
                - "id"
                - "snippet"
            comment_id (str | None):
                ID of specific `comments`. **Exactly one** of *id* or *parent_id* must be supplied.
            parent_id (str | None):
                ID of specific `comments` for which replies should be retrieved.
            max_results (int | None):
                Maximum items per page (1–50). Larger result sets require
                pagination via *page_token*.
            page_token (str | None):
                Token that identifies the results page to return.
            text_format (str | None):
                Format comments should be returned in. Acceptable values are:
                - "html"
                - "plainText"

        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the channel sections.
                - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If not exactly one of *id* or *parent_id* are supplied.
            TypeError: If an invalid type is supplied.

        References:
            https://developers.google.com/youtube/v3/docs/comments/list

        """
        if sum(map(bool, (comment_id, parent_id))) != 1:
            raise ValueError("Supply exactly one of comment_id, or parent_id=True.")

        if comment_id is not None:
            max_results = None
            page_token = None

        _COMMENTS_PARTS_ALLOWED = {"id", "snippet"}
        parts = _validate_enum("part", part, _COMMENTS_PARTS_ALLOWED)

        _TEXT_FORMATS_ALLOWED = {"html", "plainText"}
        text_formats = _validate_enum("textFormat", text_format,
                                      _TEXT_FORMATS_ALLOWED, allow_multi=False)[0]

        params = _prune_none({
            "part": ",".join(parts),
            "id": comment_id,
            "parent_id": parent_id,
            "maxResults": max_results,
            "pageToken": page_token,
            "textFormat": text_formats,
        })

        return self._list_helper("comments", params=params)

    @runtime_typecheck
    def list_comment_threads(
            self,
            *,
            part: str | Sequence[str] = "snippet",
            all_threads_related_to_channel_id: str | None = None,
            comment_thread_id: str | None = None,
            video_id: str | None = None,
            max_results: int | None = None,
            moderation_status: str | None = None,
            order: str | None = None,
            page_token: str | None = None,
            search_terms: str | None = None,
            text_format: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for the **commentThreads.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** One or more of:
                - "id"
                - "replies"
                - "snippet"

            video_id (str | None):
                Retrieve comment threads for the **video** identified by this ID.
            comment_thread_id (str | Sequence[str] | None):
                Comma-separated list of comment-thread IDs to retrieve.
            all_threads_related_to_channel_id (str | None):
                Retrieve threads for *any* video uploaded to the specified channel.

                **Exactly one** of *video_id*, *id*, or
                *all_threads_related_to_channel_id* must be supplied.
            max_results (int | None):
                Maximum items per page (1–100). Larger result sets require
                pagination via *page_token*.
            moderation_status (str | None):
                Filter threads by moderation status. Acceptable values are:
                - "heldForReview"
                - "likelySpam"
                - "published"
            order (str | None):
                Sort order of returned threads. Acceptable values are:
                - "time"
                - "relevance"
            page_token (str | None):
                Token that identifies the results page to return.
            search_terms (str | None):
                Restrict results to threads that contain the specified text.
            text_format (str | None):
                Output format for comment text. Acceptable values are:
                - "html"
                - "plainText"

        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the comment threads.
                - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If not exactly one of *video_id*, *id*, or
                *all_threads_related_to_channel_id* is supplied.
            TypeError: If an invalid type is supplied.

        References:
            https://developers.google.com/youtube/v3/docs/commentThreads/list
        """

        if sum(map(bool, (all_threads_related_to_channel_id, comment_thread_id, video_id))) != 1:
            raise ValueError("Supply exactly one of all_threads_related_to_channel_id, "
                             "comment_thread_id, or video_id.")

        if comment_thread_id is not None:
            max_results = None
            moderation_status = None
            order = None
            page_token = None
            search_terms = None

        _COMMENT_THREADS_PARTS_ALLOWED = {"id", "replies", "snippet"}
        _MODERATION_STATUS_ALLOWED = {"heldForReview", "likelySpam", "published"}
        _ORDER_ALLOWED = {"time", "relevance"}
        _TEXT_FORMATS_ALLOWED = {"html", "plainText"}

        parts = _validate_enum("part", part, _COMMENT_THREADS_PARTS_ALLOWED)
        mod_status = _validate_enum("moderationStatus", moderation_status,
                                    _MODERATION_STATUS_ALLOWED, allow_multi=False)[0]
        new_order = _validate_enum("order", order, _ORDER_ALLOWED, allow_multi=False)[0]
        text_formats = _validate_enum("textFormat", text_format,
                                      _TEXT_FORMATS_ALLOWED, allow_multi=False)[0]

        params = _prune_none({
            "part": ",".join(parts),
            "allThreadsRelatedToChannelId": all_threads_related_to_channel_id,
            "id": comment_thread_id,
            "videoId": video_id,
            "maxResults": max_results,
            "moderationStatus": mod_status,
            "order": new_order,
            "pageToken": page_token,
            "searchTerms": search_terms,
            "textFormat": text_formats,
        })

        return self._list_helper("commentThreads", params=params)

    @runtime_typecheck
    def list_i18n_languages(
            self,
            hl: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for the **i18nLanguages.list** endpoint.

        Args:
            hl (str | None):
                Language code (e.g. ``"en_US"``, ``"es_MX"``) used to
                localise the language names in the response.

        Returns:
            pandas.DataFrame: DataFrame containing the supported interface
            languages.  Each row represents one language.

        Raises:
            TypeError: If an invalid type is supplied.

        References:
            https://developers.google.com/youtube/v3/docs/i18nLanguages/list
        """

        params = _prune_none({
            "part": "snippet",
            "hl": hl,
        })

        df, _ = self._list_helper("i18nLanguages", params=params)
        return df

    @runtime_typecheck
    def list_i18n_regions(
            self,
            hl: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for the **i18nRegions.list** endpoint.

        Args:
            hl (str | None):
                Language code (e.g. ``"en_US"``, ``"es_MX"``) used to
                localise the language names in the response.

        Returns:
            pandas.DataFrame: DataFrame containing the supported YouTube regions
            (each row is an ISO 3166-1 alpha-2 country code and its human-readable
            name).

        Raises:
            TypeError: If an invalid type is supplied.

        References:
            https://developers.google.com/youtube/v3/docs/i18nRegions/list
        """

        params = _prune_none({
            "part": "snippet",
            "hl": hl,
        })

        df, _ = self._list_helper("i18nRegions", params=params)
        return df

    @runtime_typecheck
    def list_members (
            self,
            *,
            mode: str | None = None,
            max_results: int | None = None,
            page_token: str | None = None,
            has_access_to_level: str | None = None,
            filter_by_member_channel_id: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for the **members.list** endpoint.

        Args:
            mode (str | None):
                Determines which members are included. Acceptable values are:
                - "all_current" (default)
                - "updates"
            max_results (int | None):
                Maximum items per page (1–1000). Larger result sets require
                pagination via *page_token*.
            page_token (str | None):
                Token identifying the results page to return.
            has_access_to_level (str | None):
                Return only members who have at minimum access to the specified level ID.
            filter_by_member_channel_id (str | Sequence[str] | None):
                Channel IDs whose membership status should
                be checked (max 100 IDs)


        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the members.
                - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            TypeError: If an argument has an invalid type.

        References:
            https://developers.google.com/youtube/v3/docs/members/list
        """

        _MODES_ALLOWED = {"all_current", "updates"}
        modes = _validate_enum("mode", mode, _MODES_ALLOWED, allow_multi=False)[0]

        params = _prune_none({
            "part": "snippet",
            "mode": modes,
            "maxResults": max_results,
            "pageToken": page_token,
            "hasAccessToLevel": has_access_to_level,
            "filterByMemberChannelId": filter_by_member_channel_id,
        })

        return self._list_helper("members", params=params)

    @runtime_typecheck
    def list_membership_levels(
            self,
            *,
            part: str | Sequence[str] = "snippet",
    ) -> pd.DataFrame:
        """Wrapper for the **membershipsLevels.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** One or more of:
                - ``"id"``
                - ``"snippet"``

        Returns:
            pandas.DataFrame: DataFrame containing the channel’s membership levels.

        Raises:
            TypeError: If an argument has an invalid type.

        References:
            https://developers.google.com/youtube/v3/docs/membershipsLevels/list
        """
        _MEMBERSHIP_LEVELS_PARTS_ALLOWED = {"id", "snippet"}
        parts = _validate_enum("part", part, _MEMBERSHIP_LEVELS_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
        })

        df, _ = self._list_helper("membershipLevels", params=params)
        return df

    @runtime_typecheck
    def list_playlist_images(
            self,
            *,
            part: str,
            playlist_image_id: str | None = None,
            playlist_id: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            on_behalf_of_content_owner_channel: str | None = None,
            page_token: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for the **playlistImages.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.**
            playlist_image_id (str | Sequence[str] | None):
                Playlist-image IDs to retrieve.
            playlist_id (str | None):
                Playlist id to retrieve images from.

                **Exactly one** of *playlist_id* or *id* must be supplied.
            max_results (int | None):
                Maximum items per page (0–50). Larger result sets require
                pagination via *page_token*.
            on_behalf_of_content_owner (str | None):
                ID of content owner
            on_behalf_of_content_owner_channel (str | None):
                ID of the channel to which a video is being added.
            page_token (str | None):
                Token that identifies the results page to return.

        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the playlist images.
                - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If not exactly one of *playlist_id* or *id* is supplied.
            TypeError: If an invalid type is supplied.

        References:
            https://developers.google.com/youtube/v3/docs/playlistImages/list
        """

        if sum(map(bool, (playlist_image_id, playlist_id))) != 1:
            raise ValueError("Supply exactly one of playlist_image_id or playlist_id.")

        params = _prune_none({
            "part": part,
            "id": playlist_image_id,
            "playlistId": playlist_id,
            "maxResults": max_results,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "onBehalfOfContentOwnerChannel": on_behalf_of_content_owner_channel,
            "pageToken": page_token,
        })

        return self._list_helper("playlistImages", params=params)

    @runtime_typecheck
    def list_playlist_items(
            self,
            *,
            part: str | Sequence[str] = "snippet",
            playlist_item_id: str | Sequence[str] | None = None,
            playlist_id: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            page_token: str | None = None,
            video_id: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for the **playlistItems.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** One or more of:
                - "contentDetails"
                - "id"
                - "snippet"
                - "status"

            playlist_item_id (str | Sequence[str] | None):
                ID of playlist item(s)
            playlist_id (str | Sequence[str] | None):
                ID of playlist(s)

                **Exactly one** of *id* or *playlist_id* must be supplied.
            max_results (int | None):
                Maximum items per page (0–50). Larger result sets require
                pagination via *page_token*.
            on_behalf_of_content_owner (str | None):
                ID of content owner
            page_token (str | None):
                Token that identifies the results page to return.
            video_id (str | None):
                ID of specific video to return playlist items of.

        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the playlist items.
                - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If not exactly one of *playlist_id* or *id* is supplied.
            TypeError: If an invalid type is supplied.

        References:
            https://developers.google.com/youtube/v3/docs/playlistItems/list
        """

        if sum(map(bool, (playlist_item_id, playlist_id))) != 1:
            raise ValueError("Supply exactly one of playlist_item_id or playlist_id.")

        _PLAYLIST_ITEMS_PARTS_ALLOWED = {"contentDetails", "id", "snippet", "status"}
        parts = _validate_enum("part", part, _PLAYLIST_ITEMS_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "id": playlist_item_id,
            "playlistId": playlist_id,
            "maxResults": max_results,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "pageToken": page_token,
            "videoId": video_id,
        })

        return self._list_helper("playlistItems", params=params)

    @runtime_typecheck
    def list_playlists(
            self,
            *,
            part: str | Sequence[str] = "snippet",
            channel_id: str | None = None,
            playlist_id: str | None = None,
            mine: bool | None = None,
            hl: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            on_behalf_of_content_owner_channel: str | None = None,
            page_token: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for the **playlists.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** One or more of:
                - "contentDetails"
                - "id"
                - "localizations"
                - "player"
                - "snippet"
                - "status"
            channel_id (str | None):
                ID of channel to return playlist items of.
            playlist_id (str | Sequence[str] | None):
                Playlist IDs to retrieve.
            mine (bool | None):
                ``True`` to retrieve playlists owned by the authenticated user.

                **Exactly one** of *channel_id*, *id*, or *mine* must be supplied.
            hl (str | None):
                Language code (e.g. ``"en_US"``, ``"es_MX"``) used to
                localise the language names in the response.
            max_results (int | None):
                Maximum items per page (0–50). Larger result sets require
                pagination via *page_token*.
            on_behalf_of_content_owner (str | None):
                ID of content owner
            on_behalf_of_content_owner_channel (str | None):
                ID of the channel to which a video is being added.
            page_token (str | None):
                Token that identifies the results page to return.

        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the playlists.
                - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If not exactly one of *channel_id*, *playlist_id*, or *mine* is
                supplied.
            TypeError: If an invalid type is supplied.

        References:
            https://developers.google.com/youtube/v3/docs/playlists/list
        """
        if sum(map(bool, (channel_id, playlist_id, mine))) != 1:
            raise ValueError("Supply exactly one of channel_id, playlist_id, or mine=True.")

        _PLAYLISTS_PARTS_ALLOWED = {"contentDetails", "id", "localizations", "player",
                                    "snippet", "status"}
        parts = _validate_enum("part", part, _PLAYLISTS_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "channelId": channel_id,
            "id": playlist_id,
            "mine": mine,
            "hl": hl,
            "maxResults": max_results,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "onBehalfOfContentOwnerChannel": on_behalf_of_content_owner_channel,
            "pageToken": page_token,
        })

        return self._list_helper("playlists", params=params)

    @runtime_typecheck
    def list_search(
            self,
            *,
            q: str | None = None,
            for_content_owner: bool | None = None,
            for_developer: bool | None = None,
            for_mine: bool | None = None,
            channel_id: str | None = None,
            channel_type: str | None = None,
            event_type: str | None = None,
            location: str | None = None,
            location_radius: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            order: str | Sequence[str] | None = None,
            page_token: str | None = None,
            published_after: datetime | date | str | None = None,
            published_before: datetime | date | str | None = None,
            region_code: str | None = None,
            relevance_language: str | None = None,
            safe_search: str | None = None,
            topic_id: str | None = None,
            type: str | None = None,
            video_caption: str | None = None,
            video_category_id: str | None = None,
            video_definition: str | None = None,
            video_dimensions: str | None = None,
            video_duration: str | None = None,
            video_embeddable: str | None = None,
            video_license: str | None = None,
            video_paid_product_placement: str | None = None,
            video_syndicated: str | None = None,
            video_type: str | None = None,
    ) -> tuple[pd.DataFrame, str | None] :
        """Wrapper for the **search.list** endpoint.

        Args:
            q (str | None):
                Query term to search for. Use Boolean *NOT* (``-``) and *OR*
                (``|``) operators for advanced search
            for_content_owner (bool | None):
                Boolean to restrict search to only videos owned by content owner identified in
                `on_behalf_of_content_owner` parameter
            for_developer (bool | None):
                Boolean to restrict search to only videos uploaded by the developer's application or website
            for_mine (bool | None):
                Boolean to restrict search to only videos uploaded by authenticated user

                **Specify 0 or 1** of *for_content_owner*, *for_developer*, or *for_mine*
            channel_id (str | None):
                ID of channel to show results of.
            channel_type (str | None):
                Restrict search to a particular type of channel. Acceptable values are:
                - "any"
                - "show"
            event_type (str | None):
                Only include broadcasts of this type. Acceptable values are:
                    - "completed",
                    - "live",
                    - "upcoming"
                This parameter requires *type="video"*
            location (str | None):
                Latitude/longitude of search centre (e.g. ``"37.42307,-122.08427"``).
                Requires *type="video"* and *location_radius*
            location_radius (str | None):
                Distance from *location* (e.g. ``"5km"``, ``"10000ft"``)
            max_results (int | None):
                Maximum items per page (0 – 50). Larger result sets require
                pagination via *page_token*
            on_behalf_of_content_owner (str | None):
                ID of content owner
            order (str | None):
                Sort order of results. Acceptable values are:
                    - "date",
                    - "rating",
                    - "relevance",
                    - "title",
                    - "videoCount",
                    - "viewCount"
            page_token (str | None):
                Token that identifies the results page to return
            published_after (datetime | date | str | None):
                Earliest date/time (inclusive) the activity occurred.
            published_before (datetime | date | str | None):
                Latest date/time (exclusive) the activity occurred.
            region_code (str | None):
                Two-letter ISO 3166-1 alpha-2 region code used to filter results.
            relevance_language (str | None):
                Two-letter ISO 639-1 language code used to filter results.
            safe_search (str | None):
                Safe-search filtering. Acceptable values are:
                - "moderate",
                - "none",
                - "strict"
            topic_id (str | None):
                ID of topic to show results of.
            type (str | None):
                Resource type to return. Acceptable values are:
                - "video"
                - "channel"
                - "playlist"
            video_caption (str | None):
                Caption filter for video results. Acceptable values are:
                - "any",
                - "closedCaption",
                - "none"
                Requires *type="video"*
            video_category_id (str | None):
                ID of video category to show results of.
                Requires *type="video"*
            video_definition (str | None):
                Video definition to show results of. Acceptable values are:
                - "any",
                - "high",
                - "standard"
                Requires *type="video"*
            video_dimensions (str | None):
                Video dimensions to show results of. Acceptable values are:
                - "2d",
                - "3d",
                - "any"
                Requires *type="video"*
            video_duration (str | None):
                Video duration to show results of. Acceptable values are:
                - "any",
                - "long",
                - "medium",
                - "short",
                Requires *type="video"*
            video_embeddable (str | None):
                Show videos that can only be embedded into a webpage. Acceptable values are:
                - "any"
                - "true"
                Requires *type="video"*
            video_license (str | None):
                License of videos to show results of. Acceptable values are:
                - "any"
                - "creativeCommon"
                - "youtube"
                Requires *type="video"*
            video_paid_product_placement (str | None):
                Show videos that have paid product placement. Acceptable values are:
                - "any"
                - "true"
                Requires *type="video"*
            video_syndicated (str | None):
                Show videos that can be played outside YouTube.com. Acceptable values are:
                - "any"
                - "true"
                Requires *type="video"*
            video_type (str | None):
                Type of video to show results of. Acceptable values are:
                - "any"
                - "episode"
                - "movie"
                Requires *type="video"*

        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the search results.
                - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If not 0 or 1 of `for_content_owner`, `for_developer` or `for_mine` passed
            TypeError: If an argument has an invalid type.

        References:
            https://developers.google.com/youtube/v3/docs/search/list
        """

        # Verify 0 or 1 filters have been provided ----------------------------
        if sum(map(bool, (for_content_owner, for_developer, for_mine))) > 1:
            raise ValueError("Supply none or one of the following: for_content_owner, for_developer, for_mine.")

        # Verify values passed ------------------------------------------------
        channel_types = _validate_enum("channel_type", channel_type, {"any", "show"},
                                       allow_multi=False)[0] if channel_type else None
        event_types = _validate_enum("event_type", event_type, {"completed", "live", "upcoming"},
                                     allow_multi=False)[0] if event_type else None
        orders = _validate_enum("order", order,
                                {"date", "rating", "relevance", "title", "videoCount", "viewCount"},
                                allow_multi=False)[0] if order else None
        safe_searches = _validate_enum("safe_search", safe_search, {"moderate", "none", "strict"},
                                       allow_multi=False)[0] if safe_search else None
        types = _validate_enum("type", type, {"channel", "playlist", "video"},
                               allow_multi=False)[0] if type else None
        vid_captions = _validate_enum("video_caption", video_caption,
                                        {"any", "closedCaption", "none"},
                                        allow_multi=False)[0] if video_caption else None
        vid_definition = _validate_enum("video_definition", video_definition,
                                           {"any", "high", "standard"},
                                           allow_multi=False)[0] if video_definition else None
        vid_dimension = _validate_enum("video_dimensions", video_dimensions,
                                          {"2d", "3d", "any"},
                                          allow_multi=False)[0] if video_dimensions else None
        vid_duration = _validate_enum("video_duration", video_duration,
                                         {"any", "long", "medium", "short"},
                                         allow_multi=False)[0] if video_duration else None
        vid_embeddable = _validate_enum("video_embeddable", video_embeddable,
                                           {"any", "true"},
                                           allow_multi=False)[0] if video_embeddable else None
        vid_license = _validate_enum("video_license", video_license,
                                     {"any", "creativeCommon", "youtube"},
                                     allow_multi=False)[0] if video_license else None
        vid_paid_product_placement = _validate_enum("video_paid_product_placement", video_paid_product_placement,
                                                    {"any", "true"},
                                                    allow_multi=False)[0] if video_paid_product_placement else None
        vid_syndicated = _validate_enum("video_syndicated", video_syndicated,
                                        {"any", "true"},
                                        allow_multi=False)[0] if video_syndicated else None
        vid_type = _validate_enum("video_type", video_type,
                                  {"any", "episode", "movie"},
                                  allow_multi=False)[0] if video_type else None


        # Verify that type == video if other video_ args passed ---------------
        video_filters = (
            video_caption, video_category_id, video_definition, video_dimensions,
            video_duration, video_embeddable, video_license,
            video_paid_product_placement, video_syndicated, video_type
        )
        if any(v is not None for v in video_filters): types = "video"

        params = _prune_none({
            "part": "snippet",
            "q": q,
            "forContentOwner": str(for_content_owner).lower() if for_content_owner else None,
            "forDeveloper": str(for_developer).lower() if for_developer else None,
            "forMine": str(for_mine).lower() if for_mine else None,
            "channelId": channel_id,
            "channelType": channel_types,
            "eventType": event_types,
            "location": location,
            "locationRadius": location_radius,
            "maxResults": max_results,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "order": orders,
            "pageToken": page_token,
            "publishedAfter": self._iso(published_after) if published_after else None,
            "publishedBefore": self._iso(published_before) if published_before else None,
            "regionCode": region_code,
            "relevanceLanguage": relevance_language,
            "safeSearch": safe_searches,
            "topicId": topic_id,
            "type": types,
            "videoCaption": vid_captions,
            "videoCategoryId": video_category_id,
            "videoDefinition": vid_definition,
            "videoDimension": vid_dimension,
            "videoDuration": vid_duration,
            "videoEmbeddable": vid_embeddable,
            "videoLicense": vid_license,
            "videoPaidProductPlacement": vid_paid_product_placement,
            "videoSyndicated": vid_syndicated,
            "videoType": vid_type,
        })

        return self._list_helper("search", params=params)

    @runtime_typecheck
    def list_subscriptions(
            self,
            *,
            part: str | Sequence[str] = ("contentDetails", "snippet"),
            channel_id: str | None = None,
            subscription_id: str | None = None,
            mine: bool | None = None,
            my_recent_subscribers: bool | None = None,
            my_subscribers: bool | None = None,
            for_channel_id: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            on_behalf_of_content_owner_channel: str | None = None,
            order: str | None = None,
            page_token: str | None = None,
    ) -> tuple[pd.DataFrame, str] | None:
        """Wrapper for the **subscriptions.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** One or more of:
                - ``"contentDetails"``
                - ``"id"``
                - ``"snippet"``
                - ``"subscriberSnippet"``

            channel_id (str | None):
                ID of channel to get subscriptions for.
            subscription_id (str | Sequence[str] | None):
                ID of subscriptions to retrieve.
            mine (bool | None):
                ``True`` to list subscriptions of the authenticated user
            my_recent_subscribers (bool | None):
                ``True`` to list *recent* subscribers to the authenticated user
            my_subscribers (bool | None):
                ``True`` to list *all* subscribers to the authenticated user

                **Exactly one** of *channel_id*, *id*, *mine*, *my_recent_subscribers*,
                or *my_subscribers* must be supplied.
            for_channel_id (str | None):
                ID of channel to get subscriptions for.
            max_results (int | None):
                Maximum items per page (0 – 50). Larger result sets require
                pagination via *page_token*
            on_behalf_of_content_owner (str | None):
                ID of content owner
            on_behalf_of_content_owner_channel (str | None):
                ID of the channel to which a video is being added.
            order (str | None):
                Sort order of results. Acceptable values are:
                - "alphabetical",
                - "relevance",
                - "unread"
            page_token (str | None):
                Token that identifies the results page to return

        Returns:
            tuple[pandas.DataFrame, str | None]:
                - DataFrame containing the subscriptions.
                - ``nextPageToken`` if more data are available, else ``None``.

        Raises:
            ValueError: If the “exactly one” filter rule is violated.
            TypeError: If an argument has an invalid type.

        References:
            https://developers.google.com/youtube/v3/docs/subscriptions/list
        """
        if sum(map(bool, (channel_id, subscription_id, mine, my_recent_subscribers, my_subscribers))) != 1:
            raise ValueError("Supply exactly one of channel_id, subscription_id, or mine=True.")

        _SUBSCRIPTIONS_PARTS_ALLOWED = {"contentDetails", "id", "snippet", "subscriberSnippet"}
        parts = _validate_enum("part", part, _SUBSCRIPTIONS_PARTS_ALLOWED)
        orders = _validate_enum("order", order, {"alphabetical", "relevance", "unread"},
                                allow_multi=False)[0] if order else None

        params = _prune_none({
            "part": ",".join(parts),
            "channelId": channel_id,
            "id": subscription_id,
            "mine": str(mine).lower() if mine else None,
            "myRecentSubscribers": str(my_recent_subscribers).lower() if my_recent_subscribers else None,
            "mySubscribers": str(my_subscribers).lower() if my_subscribers else None,
            "forChannelId": for_channel_id,
            "maxResults": max_results,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "onBehalfOfContentOwnerChannel": on_behalf_of_content_owner_channel,
            "order": orders,
            "pageToken": page_token,
        })

        return self._list_helper("subscriptions", params=params)

    @runtime_typecheck
    def list_video_abuse_report_reasons(
            self,
            *,
            part: str | Sequence[str] = ("id", "snippet"),
            hl: str | None = None,
    ) -> pd.DataFrame:
        """
        Wrapper for the **videoAbuseReportReasons.list** endpoint.

        Args:
            part (str | Sequence[str]):
                **Required.** One or more of:
                - "id"
                - "snippet"
            hl (str | None):
                Language code (e.g. ``"en_US"``, ``"es_MX"``) used to
                localise the language names in the response.

        Returns:
            pandas.DataFrame: DataFrame containing the abuse-report reasons.

        Raises:
            TypeError: If an argument has an invalid type.

        References:
            https://developers.google.com/youtube/v3/docs/videoAbuseReportReasons/list
        """
        parts = _validate_enum("part", part, {"id", "snippet"})

        params = _prune_none({
            "part": ",".join(parts),
            "hl": hl,
        })

        df, _ = self._list_helper("videoAbuseReportReasons", params=params)
        return df

    @runtime_typecheck
    def list_video_categories(
            self,
            *,
            video_category_id: str | None = None,
            region_code: str | None = None,
            hl: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for the **videoCategories.list** endpoint.

            Args:
                video_category_id (str | Sequence[str] | None):
                    Comma-separated list of video-category IDs to retrieve
                region_code (str | None):
                    Two-letter ISO 3166-1 alpha-2 region code used to filter results.

                    **Exactly one** of *region_code* or *id* must be supplied.

                hl (str | None):
                    Language code (e.g. ``"en_US"``, ``"es_MX"``) used to
                    localise the language names in the response.

            Returns:
                pandas.DataFrame: DataFrame containing the video categories.

            Raises:
                ValueError: If both or neither of *region_code* and *id* are supplied.
                TypeError: If an argument has an invalid type.

            References:
                https://developers.google.com/youtube/v3/docs/videoCategories/list
            """
        if sum(map(bool, (video_category_id, region_code))) != 1:
            raise ValueError("Supply exactly one of *video_category_id* or *region_code*")

        params = _prune_none({
            "part": "snippet",
            "id": video_category_id,
            "regionCode": region_code,
            "hl": hl,
        })

        df, _ = self._list_helper("videoCategories", params=params)
        return df

    @runtime_typecheck
    def list_videos(
            self,
            *,
            part: str | Sequence[str] = ("contentDetails", "snippet"),
            chart: str | None = None,
            video_id: str | None = None,
            my_rating: str | None = None,
            hl: str | None = None,
            max_height: int | None = None,
            max_results: int | None = None,
            max_width: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            page_token: str | None = None,
            region_code: str | None = None,
            video_category_id: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for the **videos.list** endpoint.

            Args:
                part (str | Sequence[str]):
                    **Required.** One or more of:
                    - "contentDetails"
                    - "fileDetails"
                    - "id"
                    - "liveStreamingDetails"
                    - "localizations"
                    - "paidProductPlacementDetails"
                    - "player"
                    - "processingDetails"
                    - "recordingDetails"
                    - "snippet"
                    - "statistics"
                    - "status"
                    - "suggestions"
                    - "topicDetails"
                chart (str | None):
                    Retrieve the specified chart. Acceptable value:
                    - "mostPopular"
                video_id (str | Sequence[str] | None):
                    ID of videos to return
                my_rating (str | None):
                    Filter to videos liked or disliked by the authenticated user.
                    Acceptable values are:
                    - "like"
                    - "dislike"

                    **Exactly one** of *chart*, *id* or *my_rating* must be supplied.

                hl (str | None):
                    Language code (e.g. ``"en_US"``, ``"es_MX"``) used to
                    localise the language names in the response.
                max_height (int | None):
                    Maximum height of embedded player in the player.embedHtml property
                max_results (int | None):
                    Maximum items per page (0 – 50). Larger result sets require
                    pagination via *page_token*
                max_width (int | None):
                    Maximum width of embedded player in the player.embedHtml property
                on_behalf_of_content_owner (str | None):
                    ID of content owner
                page_token (str | None):
                    Token that identifies the results page to return.:contentReference
                region_code (str | None):
                    Two-letter ISO 3166-1 alpha-2 region code used to filter results.
                video_category_id (str | None):
                    ID of video-category ID to return.

            Returns:
                tuple[pandas.DataFrame, str | None]:
                    - DataFrame containing the videos.
                    - ``nextPageToken`` if more data are available, else ``None``.

            Raises:
                ValueError: If the “exactly one” filter rule is violated.
                TypeError: If an argument has an invalid type.

            References:
                https://developers.google.com/youtube/v3/docs/videos/list
            """

        if sum(map(bool, (chart, video_id, my_rating))) != 1:
            raise ValueError("Supply exactly one of chart, video_id, my_rating")

        if region_code is not None or video_category_id is not None:
            chart = "mostPopular"
            video_id = None
            my_rating = None

        _VIDEOS_PARTS_ALLOWED = {"contentDetails", "fileDetails", "id", "liveStreamingDetails",
                                 "localizations", "paidProductPlacementDetails", "player",
                                 "processingDetails", "recordingDetails", "snippet",
                                 "statistics", "suggestions", "topicDetails"}
        parts = _validate_enum("part", part, _VIDEOS_PARTS_ALLOWED)

        chart_val = _validate_enum("chart", chart, {"mostPopular"},
                                   allow_multi=False)[0] if chart else None
        my_ratings = _validate_enum("my_rating", my_rating, {"dislike", "like"},
                                    allow_multi=False)[0] if my_rating else None



        params = _prune_none({
            "part": ",".join(parts),
            "chart": chart_val,
            "id": video_id,
            "myRating": my_ratings,
            "hl": hl,
            "maxHeight": max_height,
            "maxResults": max_results,
            "maxWidth": max_width,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "pageToken": page_token,
            "regionCode": region_code,
            "videoCategoryId": video_category_id,
        })

        return self._list_helper("videos", params=params)

    def channel_playlists(
            self,
            part: str | Sequence[str] = "contentDetails",
            mine: bool | None = None,
            channel_id: str | None = None,
    ) -> pd.DataFrame:
        """Return **all** playlists owned by a channel.

        This helper  calls :py:meth:`playlists.list` until every page has
        been retrieved and concatenates the results into a single DataFrame.

        Args:
            part (str | Sequence[str]):
                **Required.** One or more of:
                - "contentDetails"
                - "id"
                - "localizations"
                - "player"
                - "snippet"
                - "status"
            channel_id (str | None):
                Channel whose playlists should be returned.
            mine (bool | None):
                ``True`` to fetch playlists for the authenticated user instead of
                specifying *channel_id*.

            **Exactly one** of *channel_id* or *mine* must be supplied.

        Returns:
            pandas.DataFrame: All playlists owned by the channel.

        Raises:
            ValueError: If both or neither of *channel_id* and *mine* are supplied.
            TypeError: If a parameter has an invalid type.
        """
        if sum(map(bool, (mine, channel_id))) != 1:
            raise ValueError("Supply exactly one of mine, channel_id")

        _CHANNEL_PLAYLISTS_PARTS_ALLOWED = {"contentDetails", "id", "localizations", "player",
                                            "snippet", "status"}
        parts = _validate_enum("part", part, _CHANNEL_PLAYLISTS_PARTS_ALLOWED)

        return _paged_list(self.list_playlists, part=",".join(parts), mine=mine, channel_id=channel_id)

    def playlist_videos(self, playlist_id: str,
                            part: str = "contentDetails") -> pd.DataFrame:
        """Return **all videos** contained in a playlist.

        Internally pages through :py:meth:`playlistItems.list` until every item is
        retrieved.

        Args:
            playlist_id (str):
                **Required.** ID of the playlist to get videos from.
            part (str | Sequence[str]):
                **Required.** One or more of:
                - "id"
                - "snippet"
                - "contentDetails"
                - "status"

        Returns:
            pandas.DataFrame: All videos in *playlist_id*.

        Raises:
            TypeError: If a parameter has an invalid type.
        """
        _PLAYLIST_VIDEOS_PARTS_ALLOWED = {"id", "snippet", "contentDetails", "status"}
        parts = _validate_enum("part", part, _PLAYLIST_VIDEOS_PARTS_ALLOWED)
        return _paged_list( self.list_playlist_items, part=",".join(parts), playlist_id=playlist_id)

    def channel_videos(
            self,
            mine: bool | None = None,
            channel_id: str | None = None
    ) -> pd.DataFrame:
        """
        Return **all videos** contained in a channel.

        NOTE: A bug has persisted for several years, where not all channel videos
        are returned solely by querying videos in the "uploads" playlist.
        For this reason, this function lists **ALL** playlists in a channel
        (including the "uploads" playlist), and then lists videos in each playlist
        and aggregates them into a single DataFrame.

        Args:
            channel_id (str | None):
                ID of channel whose playlists should be returned.
            mine (bool | None):
                ``True`` to fetch playlists for the authenticated user instead of
                specifying *channel_id*.

                **Exactly one** of *channel_id* or *mine* must be supplied.

        Returns:
            pandas.DataFrame: All videos in channel.

        Raises:
            TypeError: If a parameter has an invalid type.
        """
        if sum(map(bool, (mine, channel_id))) != 1:
            raise ValueError("Supply exactly one of mine, channel_id")

        # (1) uploads playlist
        uploads_df = self.list_channels(part="contentDetails", mine=mine, channel_id=channel_id)[0]
        uploads_pid = uploads_df["contentDetails.relatedPlaylists.uploads"].iloc[0]

        # (2) every playlist owned by channel
        playlist_df = self.channel_playlists(mine=mine, channel_id=channel_id)
        playlist_ids = playlist_df["id"].tolist()

        playlist_ids.append(uploads_pid)

        # (3) gather videos & dedupe
        video_frames = [ self.playlist_videos(pid) for pid in playlist_ids ]
        all_videos = pd.concat(video_frames, ignore_index=True)
        return all_videos.drop_duplicates(subset="contentDetails.videoId").reset_index(drop=True)

    def video_metadata(self, video_id: str | Sequence[str],
                       part: str | Sequence[str] = ("snippet", "contentDetails")) -> pd.DataFrame:
        """
        Return metadata for one or more videos.

        Args:
            video_id (str | Sequence[str]):
                **Required.** ID of the video(s) to get metadata for.
            part (str | Sequence[str]):
                **Required.** Most relevant parts are:
                - "contentDetails"
                - "status"
                - "snippet"

        Returns:
            pandas.DataFrame: All videos in *video_id*.

        Raises:
            TypeError: If a parameter has an invalid type.
        """
        _VIDEO_METADATA_PARTS_ALLOWED = {"id", "snippet", "contentDetails", "status"}
        parts = _validate_enum("part", part, _VIDEO_METADATA_PARTS_ALLOWED)

        ids = [video_id] if isinstance(video_id, str) else list(video_id)
        frames: list[pd.DataFrame] = []
        for chunk in self._chunk(ids, size=50):
            df, _ = self.list_videos(part=",".join(parts), video_id=",".join(chunk))
            frames.append(df)

        return pd.concat(frames, ignore_index=True).drop_duplicates(subset="id")






















