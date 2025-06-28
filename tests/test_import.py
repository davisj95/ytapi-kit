def test_public_import():
    from ytapi_kit import AnalyticsClient
    assert AnalyticsClient.__name__ == "AnalyticsClient"
