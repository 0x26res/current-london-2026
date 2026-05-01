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

GBP_UPDATE_SCHEMA_PA = pa.schema(
    [
        pa.field("product_id", pa.string()),
        pa.field("time", pa.timestamp("ns", "UTC")),
        pa.field("price", pa.float64()),
        pa.field("last_size", pa.float64()),
    ]
)

PRICE_SCHEMA = pl.from_arrow(PRICE_SCHEMA_PA.empty_table()).schema
STATUS_SCHEMA = pl.from_arrow(STATUS_SCHEMA_PA.empty_table()).schema
GBP_PRICE_SCHEMA = pl.from_arrow(GBP_UPDATE_SCHEMA_PA.empty_table()).schema
SUMMARY_SCHEMA = pl.Schema(
    {
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


def get_gbp_price(price_df: pl.DataFrame, status_df: pl.DataFrame) -> pl.DataFrame:
    return (
        price_df.join(
            status_df.select("id", "quote_currency"),
            left_on="product_id",
            right_on="id",
            how="left",
        )
        .filter(pl.col("quote_currency") == "GBP")
        .select(["product_id", "time", "price", "last_size"])
    )


def get_summary(df: pl.DataFrame) -> pl.DataFrame:
    return df.select(
        pl.col("last_size").sum().alias("volume"),
        pl.col("product_id").count().alias("trades"),
        pl.col("product_id").n_unique().alias("uniqute_product_id"),
        pl.col("time").min().alias("first_trade_at"),
        pl.col("time").max().alias("last_trade_at"),
    )


def get_percentage_change(price_df: pl.DataFrame, status_df: pl.DataFrame):
    return (
        price_df.join(status_df, left_on="product_id", right_on="id")
        .filter(pl.col("quote_currency") == "GBP")
        .group_by("product_id")
        .agg(
            first=pl.col("price").first(),
            last=pl.col("price").last(),
        )
        .select("id", percentage_change=(pl.col("last") / pl.col("first") - 1) * 100)
    )


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


PolarsDagWrapper.history = history


def get_stale_currency(price_df: pl.DataFrame, status_df: pl.DataFrame) -> pl.DataFrame:
    return (
        price_df["product_id"]
        .unique()
        .to_frame()
        .join(status_df, left_on="product_id", right_on="id", how="left")[
            "quote_currency"
        ]
        .unique()
        .sort()
        .to_frame()
    )


def simple_dag() -> Dag:
    dag = Dag()
    price_stream = dag.pl.source_table(PRICE_SCHEMA, name="price")
    status_stream = dag.pl.source_table(STATUS_SCHEMA, name="status")

    latest_status = dag.pl.last_by_keys(status_stream, ["id"])
    gbp_stream = dag.pl.table_stream(get_gbp_price, GBP_PRICE_SCHEMA).map(
        price_stream, latest_status
    )
    dag.sink("gbp", gbp_stream)
    return dag


def complex_dag() -> Dag:
    dag = Dag()
    price_stream = dag.pl.source_table(PRICE_SCHEMA, name="price")
    status_stream = dag.pl.source_table(STATUS_SCHEMA, name="status")

    latest_status = dag.pl.last_by_keys(status_stream, ["id"])
    gbp_stream = dag.pl.table_stream(get_gbp_price, GBP_PRICE_SCHEMA).map(
        price_stream, latest_status
    )
    gbp_history = dag.pl.history(
        gbp_stream,
        time_window=datetime.timedelta(minutes=60),
        timestamp_column="time",
    )

    gbp_latest = dag.pl.table_stream(get_summary, schema=SUMMARY_SCHEMA).map(
        gbp_history
    )

    dag.sink("gbp_latest", gbp_latest)
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
                print(value.to_pandas().to_markdown(index=False))


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


def main(name: str = "simple"):
    consumer_manager = ConsumerManager(
        config={
            "bootstrap.servers": "localhost:9092",
            "group.id": "current-2026",
        },
        topics=[
            SourceTopic.from_earliest("status"),
            SourceTopic.from_earliest("price"),
            # SourceTopic.from_relative_time("price", 3_600_000),  # 1 hour ago
        ],
        batch_size=100_000,
    )
    processor = DagProcessor(DAGS[name]())

    while True:
        batch = consumer_manager.poll(timeout_ms=1_000)
        if batch.num_rows > 0:
            process_batch(batch, processor)
            if consumer_manager.is_live():
                processor.print_results()


if __name__ == "__main__":
    typer.run(main)
