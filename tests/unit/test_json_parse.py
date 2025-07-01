import json, pandas as pd
from ytapi_kit._analytics import AnalyticsClient

def load_fixture(name):
    with open(f"tests/data/{name}") as f:
        return json.load(f)

def test_dataframe_types():
    raw = load_fixture("sample_geo.json")
    df = AnalyticsClient._to_dataframe(raw)
    assert pd.api.types.is_integer_dtype(df["views"])
    assert pd.api.types.is_datetime64_ns_dtype(df["day"])