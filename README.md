# ytapi-kit
*Python helpers for YouTube's Analytics, Reporting, and Data APIs.*

[![Docs](https://img.shields.io/badge/docs-latest-brightgreen)](https://davisj95.github.io/ytapi-kit/)
[![PyPI - Version](https://img.shields.io/pypi/v/ytapi-kit.svg)](https://pypi.org/project/ytapi-kit)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ytapi-kit.svg)](https://pypi.org/project/ytapi-kit)
[![tests](https://github.com/davisj95/ytapi-kit/actions/workflows/ci.yml/badge.svg)](https://github.com/davisj95/ytapi-kit/actions)


-----

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Authentication](#authentication-oauth-20)
- [Data API](#data-api)
- [Analytics API](#analytics-api)
- [Reporting API](#reporting-api)
- [Roadmap](#roadmap)
- [Contributing](#contributing)
- [License](#license)

## Overview

`ytapi-kit` is a single Python wrapper around YouTube's Data, Analytics, and Reporting APIs.

Under the hood the library is organized around three client classes - **DataClient**, **AnalyticsClient**, and **ReportingClient**, each containing 1-to-1 methods that mirror Google's REST endpoints (e.g. `reports_query()`, `list_videos()`, `list_jobs()`, etc.) 

In addition to those low-level calls, we have added functions that pre-fill the most common parameters and return tidy `pandas` DataFrames in a single line of code (e.g. `video_geographies()`, `get_latest_report()`, etc.). More details are provided in subsequent sections.

## Installation
```
python -m pip install ytapi-kit

# For the development version:
git clone https://github.com/davisj95/ytapi-kit.git
cd ytapi-kit && python -m pip install -e '.[dev]'
```
Requires Python ≥ 3.9. Dependencies (pandas, google-auth, requests) install automatically.

## Authentication (OAuth 2.0)
While Google allows several authentication methods (API key, OAuth 2.0, etc.), currently this package uses OAuth 2.0 since all three APIs support OAuth.
1. Create a project in Google Cloud Console → enable YouTube Data. Analytics, and Reporting APIs (or whichever ones are applicable for your needs).
2. Download OAuth client secrets JSON → save as `client_secrets.json`
```python
from ytapi_kit import user_session, AnalyticsClient

session = user_session("client_secrets.json")  # browser popup when authentication required
yt = AnalyticsClient(session)
```
`ytapi-kit` caches/refreshes tokens automatically (default ~/.ytapi.pickle).

## Quickstart
```python
from ytapi_kit import user_session, AnalyticsClient, DataClient, ReportingClient

session = user_session("client_secrets.json")

# 1) Analytics: fetch daily views for a video
yt_analytics = AnalyticsClient(session)
views = yt_analytics.video_stats(
    video_ids="dQw4w9WgXcQ",
    start_date="2023-01-01",
    end_date="2023-02-01",
)
print(views.head())

# 2) Data: lookup video metadata
yt_data = DataClient(session)
meta = yt_data.video_metadata("dQw4w9WgXcQ")
print(meta["title"], meta["viewCount"])

# 3) Reporting: get latest channel_basics_a2 report
yt_reporting = ReportingClient(session)
report_types = yt_reporting.get_latest_report("channel_basics_a2")
print(report_types.head())
```
---
## Data API
The [YouTube Data API](https://developers.google.com/youtube/v3/getting-started) lets you discover, inspect, create, update, or delete nearly every YouTube resource—videos, channels, playlists, comments, and more.  You interact with it through the `DataClient`, which exposes both:

- 1‑to‑1 endpoint wrappers (`list_videos()`, `list_playlists()`, `list_comments()`, etc.) for advanced users who need every optional parameter, and

- Convenience helpers like `video_metadata()` and `channel_videos()` that hide pagination and pre‑fill the most common parts.

##### What can you do?

- Search public YouTube for any query and get back the same results users see on YouTube.

- Pull public stats (views, likes, duration, thumbnails) for any video on the platform—not just your own.

- Enumerate an entire channel’s library.

Currently, only `list` endpoints have been written for this package, with others on the way.

### Examples 
#### 1. Get all of your channel videos
```python
all_vids = yt_data.channel_videos(mine=True)
```
#### 2. Search for "Never Gonna Give You Up"
```python
rick_results = yt_data.list_search(q="Never Gonna Give You Up")
```
#### 3. Show a video's metadata (Title, description, runtime, etc)
```python
vid_meta = yt_data.video_metadata(video_id="dQw4w9WgXcQ")
```

---

## Analytics API

The [YouTube Analytics API](https://developers.google.com/youtube/analytics/data_model) provides analytics that can be found in YouTube Studio, providing in-depth insights for areas such as the following:
- **Resources**
- **Geographic areas**
- **Time Periods**
- **Playback Locations**
- **Playback Details**
- **Traffic Sources**
- **Devices**

and more. For the most customization in an api request, you can call the `reports_query` method, but wrapper functions have been written to simplify calling data and making your code easier to read. Below are some examples.

### Examples
#### 1. Channel stats (all-time)
```python
df = yt_analytics.channel_stats(metrics=("views","averageViewDuration","subscribersGained"))
```
#### 2. Last month's views by country
```python
df = yt_analytics.channel_geography(
        geo_dim    ="country",
        start_date ="2025-05-01",
        end_date   ="2025-05-31",
        metrics    =("views",)          # default is views + minutesWatched
)
```
#### 3. Audience retention for a single video
```python
df = yt_analytics.video_audience_retention(
        video_ids   ="dQw4w9WgXcQ",
        audience_type="ORGANIC",        # or AD_INSTREAM, AD_INDISPLAY
        start_date  ="2025-01-01",
        end_date    ="2025-01-31",
)
```
Every helper returns a `pandas.DataFrame`.

---
## Reporting API
The [YouTube Reporting API](https://developers.google.com/youtube/reporting/v1/reports) is designed for high-volume, historical reports that are exported daily. You first create a *job*, YouTube will generate the report on its schedule, and then you download the resulting CSV file. As mentioned above, you can interact with each endpoint directly, but a convenient wrapper `get_latest_report()` combines a multi-step workflow into one easy-to-use function to get the latest report.

### Example
#### Get the latest "channel_basics_a2" report
```python
latest_report = yt_reporting.get_latest_report("channel_basics_a2")
```
---

## Roadmap
- Add remaining endpoints in YouTube Data API
- Add `Groups` and `Groupitems` endpoints in YouTube Analytics API
- Added support for service account authentication and other methods of authentication
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


```{toctree}
:hidden:

api/_data
api/_analytics
api/_reporting
```