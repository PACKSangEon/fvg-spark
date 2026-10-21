"""
Alpaca 바 딕셔너리 목록을 fixed-point long Parquet 으로 저장한다.

파티션: symbol / date (YYYY-MM-DD)
스키마: ts_micros(int64), open/high/low/close/vwap(int64 fixed-point), volume/trade_count(int64)

Java 엔진 가격 규격: 1 unit = $0.0001  →  price_long = round(float * PRICE_SCALE)
"""
from __future__ import annotations

import os
from typing import Sequence

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import timezone
from dateutil import parser as dtparser

# Parquet 스키마 — Java 엔진의 고정소수점 long 포맷과 일치
_SCHEMA = pa.schema([
    pa.field("ts_micros",    pa.int64()),   # epoch microseconds
    pa.field("open",         pa.int64()),   # fixed-point ($0.0001 단위)
    pa.field("high",         pa.int64()),
    pa.field("low",          pa.int64()),
    pa.field("close",        pa.int64()),
    pa.field("vwap",         pa.int64()),
    pa.field("volume",       pa.int64()),
    pa.field("trade_count",  pa.int32()),
    # 파티션 키 — pyarrow 가 자동으로 디렉토리를 생성한다
    pa.field("symbol",       pa.string()),
    pa.field("date",         pa.string()),  # YYYY-MM-DD
])


def _to_long(price: float, scale: int) -> int:
    return round(price * scale)


def _parse_micros(ts_str: str) -> int:
    dt = dtparser.isoparse(ts_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def bars_to_dataframe(
    symbol: str,
    bars: Sequence[dict],
    price_scale: int,
) -> pd.DataFrame:
    """Alpaca 바 목록 → pandas DataFrame (fixed-point long 변환 포함)."""
    if not bars:
        return pd.DataFrame(columns=[f.name for f in _SCHEMA])

    rows = []
    for b in bars:
        ts = b["t"]
        date_str = ts[:10]  # YYYY-MM-DD
        rows.append({
            "ts_micros":   _parse_micros(ts),
            "open":        _to_long(b["o"],  price_scale),
            "high":        _to_long(b["h"],  price_scale),
            "low":         _to_long(b["l"],  price_scale),
            "close":       _to_long(b["c"],  price_scale),
            "vwap":        _to_long(b.get("vw", b["c"]), price_scale),
            "volume":      int(b["v"]),
            "trade_count": int(b.get("n", 0)),
            "symbol":      symbol,
            "date":        date_str,
        })

    return pd.DataFrame(rows)


def write_parquet(df: pd.DataFrame, output_dir: str) -> None:
    """DataFrame 을 partitionBy(symbol, date) Parquet 으로 저장한다."""
    if df.empty:
        return

    table = pa.Table.from_pandas(df, schema=_SCHEMA, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=output_dir,
        partition_cols=["symbol", "date"],
        existing_data_behavior="overwrite_or_ignore",
        compression="snappy",
    )
