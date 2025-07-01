from ytapi_kit._analytics import AnalyticsClient
import pandas as pd

def test_builds_filters(mocker):
    dummy_df = pd.DataFrame({"country": ["US"], "views": [123]})

    stub = mocker.patch.object(AnalyticsClient, "analytics_request", return_value=dummy_df)
    yt = AnalyticsClient(session=None)

    yt.video_geography(video_ids="abc123", geo_dim="country")  # call code under test

    # Assert the wrapper passed the expected params to analytics_request
    stub.assert_called_once()
    kwargs = stub.call_args.kwargs
    assert kwargs["dimensions"] == ("video", "country")
    assert kwargs["filters"].endswith("video==abc123")  # filter string contains ID
