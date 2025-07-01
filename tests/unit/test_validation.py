import pytest
from ytapi_kit._analytics import AnalyticsClient

def test_video_geography_rejects_bad_dim():
    yt = AnalyticsClient(session=None)          # session unused in this test
    with pytest.raises(ValueError) as exc:
        yt.video_geography("abc123", geo_dim="planet")
    assert "geo_dim='planet'" in str(exc.value)
