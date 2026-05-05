import json
import polars as pl
import pytest
import pyarrow as pa
from pathlib import Path

from pytest_benchmark.fixture import BenchmarkFixture


@pytest.fixture()
def messages():
    with open(Path(__file__).parent / "data" / "prices.jsonl") as fp:
        return pa.array(list(fp.readlines())[:-1])


def run_python(messages):
    pl.DataFrame([json.loads(m.as_py()) for m in messages])


def run_polars(messages):
    pl.from_arrow(messages).str.json_decode()


def test_python(benchmark: BenchmarkFixture, messages):
    benchmark.group = 'parse_json'
    benchmark(
        run_python, messages
    )


def test_polars(benchmark: BenchmarkFixture, messages):
    benchmark.group = 'parse_json'
    benchmark(
        run_polars, messages
    )