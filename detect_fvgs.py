"""
FVG 탐지 + 완충 시뮬레이션 메인 스크립트.

설계 분리:
  - 탐지/시뮬레이션: Python ProcessPoolExecutor (JVM 의존성 없음, 빠름)
  - 분산 집계/ML: PySpark (analyze_fvgs.py 에서 별도 실행)

사용법:
    python detect_fvgs.py                     # config.py 심볼 전체
    python detect_fvgs.py --symbol AAPL       # 단일 심볼 테스트
    python detect_fvgs.py --workers 4         # 병렬 프로세스 수 지정
    python detect_fvgs.py --ttl 100           # TTL 캔들 수 변경

출력: data/fvgs_parquet/symbol=X/date=YYYY-MM-DD/*.parquet
"""
from __future__ import annotations

import argparse
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.parquet as pq

import config
import detect.fvg_pipeline as pipeline

_FVG_SCHEMA = pa.schema([
    pa.field("symbol",           pa.string()),
    pa.field("type",             pa.string()),
    pa.field("origin_ts_micros", pa.int64()),
    pa.field("formed_at_micros", pa.int64()),
    pa.field("top",              pa.int64()),
    pa.field("bottom",           pa.int64()),
    pa.field("gap",              pa.int64()),
    pa.field("gap_centi_bps",    pa.int64()),
    pa.field("state",            pa.string()),
    pa.field("fill_permille",    pa.int32()),
    pa.field("lifetime_bars",    pa.int32()),
])


def _process_one(symbol: str, ttl: int) -> dict:
    """단일 심볼을 처리하고 통계 딕셔너리를 반환한다 (subprocess 진입점)."""
    import pyarrow.parquet as pq
    import pyarrow as pa

    pipeline.TTL_BARS = ttl

    df = pq.read_table(
        config.PARQUET_DIR,
        filters=[("symbol", "=", symbol)],
    ).to_pandas()

    result = pipeline.process_symbol((symbol,), df)

    table = pa.Table.from_pandas(result, schema=_FVG_SCHEMA, preserve_index=False)
    pq.write_to_dataset(
        table,
        root_path=config.FVG_PARQUET_DIR,
        partition_cols=["symbol"],
        existing_data_behavior="delete_matching",
        compression="snappy",
    )

    return {
        "symbol": symbol,
        "bars":   len(df),
        "fvgs":   len(result),
        "full":   int((result["state"] == "FULL").sum()),
        "partial": int((result["state"] == "PARTIAL").sum()),
        "expired": int((result["state"] == "EXPIRED").sum()),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="FVG 탐지 + 완충 시뮬레이션")
    ap.add_argument("--symbol",  default=None)
    ap.add_argument("--ttl",     type=int, default=200)
    ap.add_argument("--workers", type=int, default=os.cpu_count() or 4)
    args = ap.parse_args()

    os.makedirs(config.FVG_PARQUET_DIR, exist_ok=True)
    pipeline.TTL_BARS = args.ttl

    symbols = [args.symbol] if args.symbol else config.SYMBOLS
    t0 = time.time()

    print(f"심볼 {len(symbols)}개  TTL={args.ttl}  workers={min(args.workers, len(symbols))}")

    stats = []
    with ProcessPoolExecutor(max_workers=min(args.workers, len(symbols))) as pool:
        futures = {pool.submit(_process_one, sym, args.ttl): sym for sym in symbols}
        for fut in as_completed(futures):
            sym = futures[fut]
            try:
                s = fut.result()
                fill_rate = s["full"] / s["fvgs"] * 100 if s["fvgs"] else 0
                print(f"  {s['symbol']:<6}  bars={s['bars']:>7,}  fvgs={s['fvgs']:>7,}"
                      f"  full={s['full']:>6,} ({fill_rate:5.1f}%)"
                      f"  partial={s['partial']:>5,}  expired={s['expired']:>5,}")
                stats.append(s)
            except Exception as e:
                print(f"  {sym} 실패: {e}")

    elapsed = time.time() - t0
    total_fvgs = sum(s["fvgs"] for s in stats)
    total_full = sum(s["full"] for s in stats)
    print(f"\n완료 ({elapsed:.1f}s)  총 FVG {total_fvgs:,}개"
          f"  전체 완충률 {total_full/total_fvgs*100:.1f}%"
          f"  저장: {config.FVG_PARQUET_DIR}")


if __name__ == "__main__":
    main()
