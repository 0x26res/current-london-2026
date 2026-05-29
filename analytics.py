import dataclasses
import datetime

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import typer
from beavers import Dag, TimerManager, Node
from beavers.polars_wrapper import PolarsDagWrapper
from beavers.polars_wrapper import _get_stream_schema
from kafkars import ConsumerManager, SourceTopic

GBP_TIME_WINDOW = datetime.timedelta(minutes=60)
MAX_SLIPPAGE = 0.02

PRICE_SCHEMA_PA = pa.schema(
    [
        pa.field("sequence", pa.int64()),
        pa.field("product_id", pa.string()),
        # Historic info:
        pa.field("open_24h", pa.float64()),
        pa.field("low_24h", pa.float64()),
        pa.field("high_24h", pa.float64()),
        pa.field("volume_24h", pa.float64()),
        pa.field("volume_30d", pa.float64()),
        # Bid/Off info:
        pa.field("best_bid", pa.float64()),
        pa.field("best_bid_size", pa.float64()),
        pa.field("best_ask", pa.float64()),
        pa.field("best_ask_size", pa.float64()),
        # Last trade info:
        pa.field("side", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("time", pa.timestamp("ns", "UTC")),
        pa.field("trade_id", pa.int64()),
        pa.field("last_size", pa.float64()),
    ]
)

STATUS_SCHEMA_PA = pa.schema(
    [
        pa.field("id", pa.string()),
        pa.field("base_currency", pa.string()),
        pa.field("quote_currency", pa.string()),
        pa.field("base_increment", pa.float64()),
        pa.field("quote_increment", pa.float64()),
        pa.field("display_name", pa.string()),
        pa.field("status", pa.string()),
        pa.field("status_message", pa.string()),
        pa.field("min_market_funds", pa.float64()),
        pa.field("post_only", pa.bool_()),
        pa.field("limit_only", pa.bool_()),
        pa.field("cancel_only", pa.bool_()),
        pa.field("fx_stablecoin", pa.bool_()),
        pa.field("type", pa.string()),
        pa.field("margin_enabled", pa.bool_()),
        pa.field("auction_mode", pa.bool_()),
        pa.field("max_slippage_percentage", pa.float64()),
    ]
)

ENHANCED_PRICE_SCHEMA_PA = pa.schema(
    [
        pa.field("product_id", pa.string()),
        pa.field("time", pa.timestamp("ns", "UTC")),
        pa.field("price", pa.float64()),
        pa.field("last_size", pa.float64()),
        pa.field("quote_currency", pa.string()),
    ]
)

PRICE_SCHEMA = pl.from_arrow(PRICE_SCHEMA_PA.empty_table()).schema
STATUS_SCHEMA = pl.from_arrow(STATUS_SCHEMA_PA.empty_table()).schema
ENHANCED_PRICE_SCHEMA = pl.from_arrow(ENHANCED_PRICE_SCHEMA_PA.empty_table()).schema
SUMMARY_SCHEMA = pl.Schema(
    {
        "quote_currency": pl.String,
        "volume": pl.Float64,
        "trades": pl.Int64,
        "unique_product_id": pl.Int64,
        "first_trade_at": pl.Datetime("ns", "UTC"),
        "last_trade_at": pl.Datetime("ns", "UTC"),
    }
)


def batch_to_df(batch: pa.Table, topic: str, schema: pl.Schema) -> pl.DataFrame:
    topic_batch = batch.filter(pc.field("topic") == topic)
    raw_struct = pl.Struct(
        {
            name: pl.String if dtype == pl.Float64 else dtype
            for name, dtype in schema.items()
        }
    )
    casts = [
        pl.when(pl.col(name) == "")
        .then(pl.lit(None, pl.String()))
        .otherwise(pl.col(name))
        .alias(name)
        for name, dtype in schema.items()
        if dtype == pl.Float64
    ]

    return (
        pl.from_arrow(topic_batch.select(["value"]))
        .with_columns(pl.col("value").cast(pl.String).str.json_decode(dtype=raw_struct))
        .unnest("value")
        .with_columns(casts)
        .cast(schema)
    )


def batch_to_dfs(batch: pa.Table, **schemas: pa.Schema) -> tuple[pa.Table, ...]:
    return tuple(batch_to_df(batch, topic, schema) for topic, schema in schemas.items())


def get_enhanced_price(price_df: pl.DataFrame, status_df: pl.DataFrame) -> pl.DataFrame:
    return (
        price_df.join(
            status_df.select("id", "quote_currency", "max_slippage_percentage"),
            left_on="product_id",
            right_on="id",
            how="left",
        )
        .filter(pl.col("max_slippage_percentage") >= MAX_SLIPPAGE)
        .select(["product_id", "time", "price", "last_size", "quote_currency"])
    )


def get_summary(df: pl.DataFrame) -> pl.DataFrame:
    return df.group_by("quote_currency").agg(
        pl.col("last_size").sum().alias("volume"),
        pl.col("product_id").count().alias("trades"),
        pl.col("product_id").n_unique().alias("unique_product_id"),
        pl.col("time").min().alias("first_trade_at"),
        pl.col("time").max().alias("last_trade_at"),
    )

def get_filtered_summary(df: pl.DataFrame, stream: pl.DataFrame) -> pl.DataFrame:
    return df.filter(
        pl.col("quote_currency").is_in(stream["quote_currency"].unique())
    ).pipe(get_summary)

@dataclasses.dataclass
class History:
    state: pl.DataFrame
    time_window: datetime.timedelta
    timestamp_column: str

    def __call__(
        self,
        now,
        timer_manager: TimerManager,
        new_data: pl.DataFrame,
    ) -> pl.DataFrame:
        self.state = pl.concat([self.state, new_data]).filter(
            pl.col(self.timestamp_column) > (now - self.time_window)
        )

        if not self.state.is_empty():
            timer_manager.set_next_timer(
                pd.Timestamp(self.state[self.timestamp_column].min() + self.time_window)
            )

        return self.state


def history(
    self: PolarsDagWrapper,
    stream: Node,
    time_window: datetime.timedelta,
    timestamp_column: str,
):
    dag = self._dag
    schema = _get_stream_schema(stream)
    return dag.state(
        History(
            state=schema.to_frame(),
            time_window=time_window,
            timestamp_column=timestamp_column,
        )
    ).map(
        dag.now(),
        dag.timer_manager(),
        stream,
    )



def stream_series(
    self: PolarsDagWrapper,
    transformation,
    dtype,
):
    return self._dag.stream(
        transformation,
        empty=pl.Series(dtype=dtype)
    )


PolarsDagWrapper.history = history
PolarsDagWrapper.stream_series = stream_series



def simple_dag() -> Dag:
    dag = Dag()
    price_stream = dag.pl.source_table(PRICE_SCHEMA, name="price")
    status_stream = dag.pl.source_table(STATUS_SCHEMA, name="status")

    latest_status = dag.pl.last_by_keys(status_stream, ["id"])
    enhanced_stream = dag.pl.table_stream(get_enhanced_price, ENHANCED_PRICE_SCHEMA).map(
        price_stream, latest_status
    )
    dag.sink("enhanced", gbp_stream)
    return dag


def complex_dag() -> Dag:
    dag = Dag()
    price_stream = dag.pl.source_table(PRICE_SCHEMA, name="price")
    status_stream = dag.pl.source_table(STATUS_SCHEMA, name="status")

    latest_status = dag.pl.last_by_keys(status_stream, ["id"])
    enhanced_stream = dag.pl.table_stream(
        get_enhanced_price, ENHANCED_PRICE_SCHEMA
    ).map(price_stream, latest_status)

    enhanced_history = dag.pl.history(
        enhanced_stream,
        time_window=GBP_TIME_WINDOW,
        timestamp_column="time",
    )

    summary = dag.pl.table_stream(
        get_filtered_summary,
        schema=SUMMARY_SCHEMA
    ).map(enhanced_history, enhanced_stream)

    dag.sink("summary", summary)
    return dag


@dataclasses.dataclass(frozen=True)
class DagProcessor:
    dag: Dag

    def __call__(self, **kwargs: pl.DataFrame):
        for name, df in kwargs.items():
            self.dag.get_sources()[name].set_stream(df)

        self.dag.execute(pd.Timestamp.now("UTC"))

    def print_results(self):
        for sink in self.dag.get_sinks().values():
            if sink[0].get_cycle_id() == self.dag.get_cycle_id():
                value = sink[0].get_sink_value()
                print(value.write_ndjson(), end="", flush=True)


def process_batch(batch: pa.Table, processor: DagProcessor) -> None:
    status, price = batch_to_dfs(batch, status=STATUS_SCHEMA, price=PRICE_SCHEMA)
    processor(
        status=pl.from_arrow(status),
        price=pl.from_arrow(price),
    )


DAGS = {
    "simple": simple_dag,
    "complex": complex_dag,
}


def main(dag: str = "simple"):
    consumer_manager = ConsumerManager(
        config={
            "bootstrap.servers": "localhost:9092",
            "group.id": "current-2026",
        },
        topics=[
            SourceTopic.from_earliest("status"),
            SourceTopic.from_latest("price")
            if dag == "simple"
            else SourceTopic.from_relative_time("price", int(GBP_TIME_WINDOW.total_seconds() * 1000)),
        ],
        batch_size=1_000_000,
    )
    processor = DagProcessor(DAGS[dag]())

    while True:
        batch = consumer_manager.poll(timeout_ms=1_000)
        if batch.num_rows > 0:
            process_batch(batch, processor)
            if consumer_manager.is_live():
                processor.print_results()


if __name__ == "__main__":
    typer.run(main)
