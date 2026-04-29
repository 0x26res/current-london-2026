import datetime

import polars as pl
from analytics import batch_to_df
import pyarrow as pa


def test_batch_to_df():
    df = batch_to_df(
        pa.record_batch(
            {
                "timestamp": [datetime.datetime(2025, 10, 10)],
                "value": [b'{"foo": ""}'],
                "topic": ["topic1"],
            }
        ),
        topic="topic1",
        schema=pl.Schema({"foo": pl.Float64}),
    )
    print(df)
    assert isinstance(df, pl.DataFrame)
