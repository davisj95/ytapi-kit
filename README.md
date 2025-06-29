# ytapi-kit
*Typed Python helpers for the YouTube Analytics API – the numbers you see in Studio, now in Pandas.*


[![PyPI - Version](https://img.shields.io/pypi/v/ytapi-kit.svg)](https://pypi.org/project/ytapi-kit)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ytapi-kit.svg)](https://pypi.org/project/ytapi-kit)
[![tests](https://github.com/davisj95/ytapi-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/davisj95/ytapi-kit/actions)


-----

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Authentication](#authentication-oauth-20)
- [Details](#feature-table)
- [Examples](#examples)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Overview

`ytapi-kit` pulls **YouTube Studio analytics** directly into Python so you can analyse, join, or visualise without copy-pasting CSVs.

**Data you can pull:**

- **Resources**
- **Geographic areas**
- **Time Periods**
- **Playback Locations**
- **Playback Details**
- **Traffic Sources**
- **Devices**
- **Demographics**
- **Engagement & Content Sharing**
- **Audience Retention**
- **Live Streaming**
- **Membership Cancellations**
- **Ad Performance**

```python
from ytapi_kit import AnalyticsClient, user_session

yt = AnalyticsClient(user_session("client_secrets.json"))
df = yt.channel_time_period(time_period="day", metrics=("views",), last_n_days=30)
print(df.tail())
```
*Details can be found in the [Details](#details) section*

---

## Installation
```
python -m pip install ytapi-kit
```
### Development Install
```
git clone https://github.com/davisj95/ytapi-kit.git
cd ytapi-kit && python -m pip install -e '.[dev]'
```
Requires Python ≥ 3.9. Dependencies (pandas, google-auth, requests) install automatically.

## Authentication (OAuth 2.0)
1. Create a project in Google Cloud Console → enable YouTube Analytics API
2. Download OAuth client secrets JSON → save as `client_secrets.json`
```python
from ytapi_kit import user_session, AnalyticsClient

session = user_session("client_secrets.json")  # browser popup on first run
yt = AnalyticsClient(session)
```

`ytapi-kit` caches/refreshes tokens automatically (default ~/.ytapi_kit_token.pickle).

## Feature Table
| Data&nbsp;Type | What the helper returns (key args in *italics*)                                                                                                      | Functions |
|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------|-----------|
| Geography | Metrics broken down by country / province / DMA / city &nbsp;(*geo_dim="country"\|…*). Can also filter other requests by "continent" or "subContinent" | `video_geography`, `channel_geography` |
| Playback&nbsp;location | Where viewers watched—YouTube, embedded, etc. &nbsp;(*detail=True* drills into embedded domains)                                                     | `video_playback_location`, `channel_playback_location` |
| Playback&nbsp;details | Split stats by `creatorContentType`, `liveOrOnDemand`, `subscribedStatus`, `youtubeProduct` &nbsp;(*detail="liveOrOnDemand"* by default)             | `video_playback_details`, `channel_playback_details` |
| Time&nbsp;period | Daily / monthly aggregates for any metrics &nbsp;(*time_period="day"&#124;"month"*)                                                                        | `video_time_period`, `channel_time_period` |
| Traffic&nbsp;sources | High-level sources (YT_SEARCH, RELATED_VIDEO…) or fine-grain detail by including the *detail* argument, such as  *detail="YT_SEARCH"*                | `video_traffic_sources`, `channel_traffic_sources` |
| Devices | Viewer device type and/or OS &nbsp;(*device_info=("deviceType","operatingSystem")*)                                                                  | `video_devices`, `channel_devices` |
| Demographics | Audience age groups and/or gender                                                                                                                    | `video_demographics`, `channel_demographics` |
| Audience&nbsp;retention | Watch-ratio curve by elapsed-time bucket &nbsp;(*audience_type="ORGANIC"\|AD_INSTREAM…*)                                                             | `video_audience_retention` |
| Sharing&nbsp;services | Which social / messaging platforms drove shares                                                                                                      | `video_sharing_services`, `channel_sharing_services` |
| Ad&nbsp;performance | Revenue / CPM by ad type (display, skippable, bumper…)                                                                                               | `channel_ad_performance` |
| Top&nbsp;videos | Best-performing videos within a playlist or channel                                                                                                  | `playlist_top_videos`, `channel_top_videos` |
| Generic&nbsp;stats | Catch-all wrappers for any custom metrics × dimensions                                                                                               | `video_stats`, `channel_stats`|

See **docstrings** (`help(AnalyticsClient)`) or the [API reference](docs/API.md) for the full helper list.


## Examples
### 1. Channel stats (all-time)
```python
df = yt.channel_stats(metrics=("views","averageViewDuration","subscribersGained"))
```
### 2. Last month's views by country
```python
df = yt.channel_geography(
        geo_dim    ="country",
        start_date ="2025-05-01",
        end_date   ="2025-05-31",
        metrics    =("views",)          # default is views + minutesWatched
)
```
### 3. Audience retention for a single video
```python
df = yt.video_audience_retention(
        video_ids   ="dQw4w9WgXcQ",
        audience_type="ORGANIC",        # or AD_INSTREAM, AD_INDISPLAY
        start_date  ="2025-01-01",
        end_date    ="2025-01-31",
)
```
### 4. Top videos in a playlist
```python
df = yt.playlist_top_videos(
        ["PL9tY0BWXOZFtQ-GG8X2E8oia-MfeLeGKv"],
        metrics=("views", "likes", "comments"),
        start_date="2024-01-01",
        end_date="2024-06-30",
)
```

Every helper returns a **typed** `pandas.DataFrame` ready for analysis or plotting.

## Roadmap
- YouTube Data API implementation
- YouTube Reporting API implementation
- Added support for service account authentication
- Potential CLI wrapper: `ytapi-kit geostats video_id --last 30d --csv out.csv`

Up-vote an issue or open a PR to help me prioritize

## Contributing
1. Fork: `git clone`
2. `python -m pip install -e '.[dev]'`
3. `pytest && ruff check .`
4. Submit a pull-request

Even small tweaks are welcome.

## License
`ytapi-kit` is released under the MIT License (see`LICENSE`)