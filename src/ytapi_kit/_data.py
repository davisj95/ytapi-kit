from __future__ import annotations

import re
from typing import Mapping, MutableMapping, Sequence
import pandas as pd
from datetime import datetime, date

from ._errors import *
from ._util import *

__all__ = ["DataClient"]

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
        """Wrapper for **activities.list**.

        Google reference → https://developers.google.com/youtube/v3/docs/activities/list
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
            id: str | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for **captions.list**."""

        _CAPTION_PARTS_ALLOWED = {"id", "snippet"}
        parts = _validate_enum("part", part, _CAPTION_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "videoId": video_id,
            "id": id,
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
            id: str | None = None,
            managed_by_me: bool | None = None,
            mine: bool | None = None,
            hl: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            page_token: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for **channels.list**."""
        if sum(map(bool, (for_handle, for_username, id, managed_by_me, mine))) != 1:
            raise ValueError("Supply exactly one of for_handle, for_username, id, managed_by_me, mine.")

        _CHANNEL_PARTS_ALLOWED = {"auditDetails", "brandingSettings", "contentDetails",
                                  "contentOwnerDetails", "id", "localizations", "snippet",
                                  "statistics", "status", "topicDetails"}
        parts = _validate_enum("part", part, _CHANNEL_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "forHandle": for_handle,
            "forUsername": for_username,
            "id": id,
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
            id: str | None = None,
            mine: bool | None = None,
            hl: str | None = None,
            on_behalf_of_content_owner: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for **channel sections.list**."""
        if sum(map(bool, (channel_id, id, mine))) != 1:
            raise ValueError("Supply exactly one of channel_id, id, or mine=True.")

        _CHANNEL_SECTION_PARTS_ALLOWED = {"contentDetails", "id", "snippet"}
        parts = _validate_enum("part", part, _CHANNEL_SECTION_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "channelId": channel_id,
            "id": id,
            "mine": str(mine).lower() if mine else None,
            "hl": hl,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
        })

        return self._list_helper("channelSections", params=params)

    @runtime_typecheck
    def list_comments(
            self,
            *,
            part: str | Sequence[str] = ("contentDetails", "snippet"),
            id: str | None = None,
            parent_id: str | None = None,
            max_results: int | None = None,
            page_token: str | None = None,
            text_format: str | None = None,

    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for **comments.list**."""
        if sum(map(bool, (id, parent_id))) != 1:
            raise ValueError("Supply exactly one of id, or parent_id=True.")

        if id is not None:
            max_results = None
            page_token = None

        _COMMENTS_PARTS_ALLOWED = {"id", "snippet"}
        parts = _validate_enum("part", part, _COMMENTS_PARTS_ALLOWED)

        _TEXT_FORMATS_ALLOWED = {"html", "plainText"}
        text_formats = _validate_enum("textFormat", text_format,
                                      _TEXT_FORMATS_ALLOWED, allow_multi=False)[0]

        params = _prune_none({
            "part": ",".join(parts),
            "id": id,
            "parent_id": parent_id,
            "maxResults": max_results,
            "pageToken": page_token,
            "textFormat": text_formats,
        })

        return self._list_helper("comments", params=params)

    @runtime_typecheck
    def list_comment_treads(
            self,
            *,
            part: str | Sequence[str] = "snippet",
            all_threads_related_to_channel_id: str | None = None,
            id: str | None = None,
            video_id: str | None = None,
            max_results: int | None = None,
            moderation_status: str | None = None,
            order: str | None = None,
            page_token: str | None = None,
            search_terms: str | None = None,
            text_format: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for **comment threads.list**."""

        if sum(map(bool, (all_threads_related_to_channel_id, id, video_id))) != 1:
            raise ValueError("Supply exactly one of all_threads_related_to_channel_id, id, or video_id.")

        if id is not None:
            max_results = None,
            moderation_status = None,
            order = None,
            page_token = None,
            search_terms = None

        _COMMENT_TREADS_PARTS_ALLOWED = {"id", "replies", "snippet"}
        _MODERATION_STATUS_ALLOWED = {"heldForReview", "likelySpam", "published"}
        _ORDER_ALLOWED = {"time", "relevance"}
        _TEXT_FORMATS_ALLOWED = {"html", "plainText"}

        parts = _validate_enum("part", part, _COMMENT_TREADS_PARTS_ALLOWED)
        mod_status = _validate_enum("moderationStatus", moderation_status,
                                    _MODERATION_STATUS_ALLOWED, allow_multi=False)[0]
        new_order = _validate_enum("order", order, _ORDER_ALLOWED, allow_multi=False)[0]
        text_formats = _validate_enum("textFormat", text_format,
                                      _TEXT_FORMATS_ALLOWED, allow_multi=False)[0]

        params = _prune_none({
            "part": ",".join(parts),
            "allThreadsRelatedToChannelId": all_threads_related_to_channel_id,
            "id": id,
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
    def list_i18nLanguages(
            self,
            part: str = "snippet",
            hl: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for **i18nLanguages.list**."""
        if part != "snippet":
            raise ValueError("part must be 'snippet'")

        params = _prune_none({
            part: part,
            "hl": hl,
        })

        df, _ = self._list_helper("i18nLanguages", params=params)
        return df

    @runtime_typecheck
    def list_i18nRegions(
            self,
            part: str = "snippet",
            hl: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for **i18nRegions.list**."""
        if part != "snippet":
            raise ValueError("part must be 'snippet'")

        params = _prune_none({
            part: part,
            "hl": hl,
        })

        df, _ = self._list_helper("i18nRegions", params=params)
        return df

    @runtime_typecheck
    def list_members (
            self,
            *,
            part: str = "snippet",
            mode: str | None = None,
            max_results: int | None = None,
            page_token: str | None = None,
            has_access_to_level: str | None = None,
            filter_by_member_channel_id: str | None = None,
    ) -> pd.DataFrame:
        """Wrapper for **members.list**."""

        if part != "snippet":
            raise ValueError("part must be 'snippet'")

        _MODES_ALLOWED = {"all_current", "updates"}
        modes = _validate_enum("mode", mode, _MODES_ALLOWED, allow_multi=False)[0]

        params = _prune_none({
            part: part,
            "mode": modes,
            "maxResults": max_results,
            "pageToken": page_token,
            "hasAccessToLevel": has_access_to_level,
            "filterByMemberChannelId": filter_by_member_channel_id,
        })

        df, _ = self._list_helper("members", params=params)
        return df

    @runtime_typecheck
    def list_membership_levels(
            self,
            *,
            part: str | Sequence[str] = "snippet",
    ) -> pd.DataFrame:
        """Wrapper for **membershipLevels.list**."""
        _MEMBERSHIP_LEVELS_PARTS_ALLOWED = {"id", "snippet"}
        parts = _validate_enum("part", part, _MEMBERSHIP_LEVELS_PARTS_ALLOWED)

        params = _prune_none({
            part: ",".join(parts),
        })

        df, _ = self._list_helper("membershipLevels", params=params)
        return df

    @runtime_typecheck
    def list_playlist_images(
            self,
            *,
            part: str,
            id: str | None = None,
            playlist_id: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            on_behalf_of_content_owner_channel: str | None = None,
            page_token: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for **playlistImages.list**."""

        if sum(map(bool, (id, playlist_id))) != 1:
            raise ValueError("Supply exactly one of id or playlist_id.")

        params = _prune_none({
            "part": part,
            "id": id,
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
            id: str | None = None,
            playlist_id: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            page_token: str | None = None,
            video_id: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for **playlistItems.list**."""

        if sum(map(bool, (id, playlist_id))) != 1:
            raise ValueError("Supply exactly one of id or playlist_id.")

        _PLAYLIST_ITEMS_PARTS_ALLOWED = {"contentDetails", "id", "snippet", "status"}
        parts = _validate_enum("part", part, _PLAYLIST_ITEMS_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "id": id,
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
            id: str | None = None,
            mine: bool | None = None,
            hl: str | None = None,
            max_results: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            on_behalf_of_content_owner_channel: str | None = None,
            page_token: str | None = None,
    ) -> tuple[pd.DataFrame, str | None]:
        """Wrapper for **playlists.list**."""
        if sum(map(bool, (channel_id, id, mine))) != 1:
            raise ValueError("Supply exactly one of channel_id, id, or mine=True.")

        _PLAYLISTS_PARTS_ALLOWED = {"contentDetails", "id", "localizations", "player",
                                    "snippet", "status"}
        parts = _validate_enum("part", part, _PLAYLISTS_PARTS_ALLOWED)

        params = _prune_none({
            "part": ",".join(parts),
            "channelId": channel_id,
            "id": id,
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
            part: str = "snippet",
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
    ) -> tuple[pd.DataFrame, str | None] :
        """Wrapper for **search.list**."""

        # Verify part == 'snippet'---------------------------------------------
        if part != "snippet":
            raise ValueError("part must be 'snippet'")

        # Verify 0 or 1 filters have been provided ----------------------------
        if sum(map(bool, (for_content_owner, for_developer, for_mine))) > 1:
            raise ValueError("Supply none or one of the following: for_content_owner, for_developer, for_mine.")

        # Verify values passed ------------------------------------------------
        channel_types = _validate_enum("channel_type", channel_type, {"any, show"},
                                       allow_multi=False)[0] if channel_type else None
        event_types = _validate_enum("event_type", event_type, {"completed", "live", "upcoming"},
                                     allow_multi=False)[0] if event_type else None
        orders = _validate_enum("order", order,
                                {"date", "rating", "relevance", "title","videoCount", "viewCount"},
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

        # Verify that type == video if other video_ args passed ---------------
        video_filters = (
            video_caption, video_category_id, video_definition, video_dimensions,
            video_duration, video_embeddable, video_license,
            video_paid_product_placement, video_syndicated,
        )
        if any(v is not None for v in video_filters): type = "video"

        params = _prune_none({
            "part": part,
            "q": q,
            "forContentOwner": str(for_content_owner).lower() if for_content_owner else None,
            "forDeveloper": str(for_developer).lower() if for_developer else None,
            "forMine": str(for_mine).lower() if for_mine else None,
            "channelId": channel_id,
            "channelType": channel_type,
            "eventType": event_type,
            "location": location,
            "locationRadius": location_radius,
            "maxResults": max_results,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "order": order,
            "pageToken": page_token,
            "publishedAfter": self._iso(published_after) if published_after else None,
            "publishedBefore": self._iso(published_before) if published_before else None,
            "regionCode": region_code,
            "relevanceLanguage": relevance_language,
            "safeSearch": safe_search,
            "topicId": topic_id,
            "type": type,
            "videoCaption": video_caption,
            "videoCategoryId": video_category_id,
            "videoDefinition": video_definition,
            "videoDimension": video_dimensions,
            "videoDuration": video_duration,
            "videoEmbeddable": video_embeddable,
            "videoLicense": video_license,
            "videoPaidProductPlacement": video_paid_product_placement,
            "videoSyndicated": video_syndicated,
        })

        return self._list_helper("search", params=params)

    @runtime_typecheck
    def list_subscriptions(
            self,
            *,
            part: str | Sequence[str] = ("contentDetails", "snippet"),
            channel_id: str | None = None,
            id: str | None = None,
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
        """Wrapper for **list.subscriptions**."""
        if sum(map(bool, (channel_id, id, mine, my_recent_subscribers, my_subscribers))) != 1:
            raise ValueError("Supply exactly one of channel_id, id, or mine=True.")

        _SUBSCRIPTIONS_PARTS_ALLOWED = {"contentDetails", "id", "snippet", "subscriberSnippet"}
        parts = _validate_enum("part", part, _SUBSCRIPTIONS_PARTS_ALLOWED)
        orders = _validate_enum("order", order, {"alphabetical", "relevance", "unread"},
                                allow_multi=False)[0] if order else None

        params = _prune_none({
            "part": ",".join(parts),
            "channelId": channel_id,
            "id": id,
            "mine": str(mine).lower() if mine else None,
            "myRecentSubscribers": str(my_recent_subscribers).lower() if my_recent_subscribers else None,
            "mySubscribers": str(my_subscribers).lower() if my_subscribers else None,
            "forChannelId": for_channel_id,
            "maxResults": max_results,
            "onBehalfOfContentOwner": on_behalf_of_content_owner,
            "onBehalfOfContentOwnerChannel": on_behalf_of_content_owner_channel,
            "order": order,
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
            part: str = "snippet",
            id: str | None = None,
            region_code: str | None = None,
            hl: str | None = None,
    ) -> pd.DataFrame:

        if sum(map(bool, (id, region_code))) != 1:
            raise ValueError("Supply exactly one of id, region_code")

        if part != "snippet":
            raise ValueError("Supply must equal 'snippet'")

        params = _prune_none({
            "part": part,
            "id": id,
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
            id: str | None = None,
            my_rating: str | None = None,
            hl: str | None = None,
            max_height: int | None = None,
            max_results: int | None = None,
            max_width: int | None = None,
            on_behalf_of_content_owner: str | None = None,
            page_token: str | None = None,
            region_code: str | None = None,
            video_category_id: str | None = None,
    ) -> tuple[pd.DataFrame, str] | None:

        if sum(map(bool, (chart, id, my_rating))) != 1:
            raise ValueError("Supply exactly one of chart, id, my_rating")

        _VIDEOS_PARTS_ALLOWED = {"contentDetails", "fileDetails", "id", "liveStreamingDetails",
                                 "localizations", "paidProductPlacementDetails", "player",
                                 "processingDetails", "recordingDetails", "snippet",
                                 "statistics", "suggestions", "topicDetails"}
        parts = _validate_enum("part", part, _VIDEOS_PARTS_ALLOWED)

        chart_val = _validate_enum("chart", chart, {"mostPopular"},
                                   allow_multi=False)[0] if chart else None
        my_ratings = _validate_enum("my_rating", my_rating, {"dislike", "like"},
                                    allow_multi=False)[0] if my_rating else None

        if region_code is not None or video_category_id is not None:
            chart_val = "mostPopular"
            id = None
            my_rating = None

        params = _prune_none({
            "part": ",".join(parts),
            "chart": chart_val,
            "id": id,
            "myRating": my_rating,
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





















