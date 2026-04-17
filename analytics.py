import dataclasses

import polars as pl
import pyarrow as pa
import tabulate
import pyarrow.compute as pc
from kafkars import ConsumerManager, SourceTopic

from util.json_util import json_to_table

TICKER_SCHEMA = pa.schema(
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

STATUS_SCHEMA = pa.schema(
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


def batch_to_table(batch: pa.Table, topic: str, schema: pa.Schema) -> pa.Table:
    topic_batch = batch.filter(pc.field("topic") == topic)
    return json_to_table(topic_batch["value"], schema)


def batch_to_tables(batch: pa.Table, **schemas: pa.Schema) -> tuple[pa.Table, ...]:
    return tuple(
        batch_to_table(batch, topic, schema) for topic, schema in schemas.items()
    )


def get_gbp_updates(status_df: pl.DataFrame, ticker_df: pl.DataFrame) -> pl.DataFrame:
    return (
        ticker_df.join(
            status_df.select("id", "quote_currency"),
            left_on="product_id",
            right_on="id",
            how="left",
        )
        .filter(pl.col("quote_currency") == "GBP")
        .select(["product_id", "time", "best_bid", "best_ask"])
        .group_by(["product_id"])
        .last()
    )


@dataclasses.dataclass()
class BatchProcessor:
    status_df: pl.DataFrame = dataclasses.field(
        default_factory=lambda: pl.from_arrow(STATUS_SCHEMA.empty_table())
    )

    def __call__(self, status_df: pl.DataFrame, ticker_df: pl.DataFrame):
        if not status_df.is_empty():
            self.status_df = (
                pl.concat([self.status_df, status_df]).group_by("id").last()
            )
        gbp_updates = get_gbp_updates(self.status_df, ticker_df)
        if not gbp_updates.is_empty():
            print("")
            print(
                tabulate.tabulate(
                    gbp_updates.rows(), headers=gbp_updates.columns, tablefmt="pipe"
                )
            )


def process_batch(batch: pa.Table, processor: BatchProcessor) -> None:
    status, ticker = batch_to_tables(batch, status=STATUS_SCHEMA, ticker=TICKER_SCHEMA)
    processor(
        status_df=pl.from_arrow(status),
        ticker_df=pl.from_arrow(ticker),
    )


def main():
    consumer_manager = ConsumerManager(
        config={
            "bootstrap.servers": "localhost:9092",
            "group.id": "current-2026",
        },
        topics=[
            SourceTopic.from_earliest("status"),
            SourceTopic.from_relative_time("ticker", 3600_000),  # 1 hour ago
        ],
    )
    processor = BatchProcessor()

    while True:
        batch = consumer_manager.poll(
            timeout_ms=1_000,
        )
        if batch.num_rows > 0:
            process_batch(batch, processor)


if __name__ == "__main__":
    main()
