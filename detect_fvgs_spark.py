"""
PySpark 버전 FVG 탐지 실행 스크립트 (Window + lag, Python UDF 없음).

numpy 버전(detect_fvgs.py)과 같은 입력을 읽어 같은 스키마로 저장한다.
두 결과는 compare_spark_numpy.py 로 비교한다.

사용법:
    python detect_fvgs_spark.py                      # config.py 심볼 전체, local[*]
    python detect_fvgs_spark.py --symbol AAPL        # 단일 심볼
    python detect_fvgs_spark.py --master local[1]    # 코어 1개 (처리 시간 비교용)
    python detect_fvgs_spark.py --ttl 200

출력: data/fvgs_spark_parquet/symbol=X/*.parquet
"""
from __future__ import annotations

import argparse
import time

import config
from spark_utils import get_spark
from detect.spark_detect import detect_fvgs

SPARK_FVG_DIR = "data/fvgs_spark_parquet"


def main() -> None:
    ap = argparse.ArgumentParser(description="PySpark FVG 탐지 (Window + lag)")
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--ttl", type=int, default=200)
    ap.add_argument("--master", default="local[*]")
    ap.add_argument("--outdir", default=SPARK_FVG_DIR)
    args = ap.parse_args()

    spark = get_spark("fvg-detect-spark", master=args.master)

    from pyspark.sql import functions as F

    t0 = time.time()
    bars = spark.read.parquet(config.PARQUET_DIR)
    symbols = [args.symbol] if args.symbol else config.SYMBOLS
    bars = bars.filter(F.col("symbol").isin(symbols))

    fvgs = detect_fvgs(bars, ttl=args.ttl)
    fvgs.write.mode("overwrite").partitionBy("symbol").parquet(args.outdir)
    elapsed = time.time() - t0

    saved = spark.read.parquet(args.outdir)
    summary = (
        saved.groupBy("symbol")
        .agg(
            F.count("*").alias("fvgs"),
            F.sum(F.when(F.col("state") == "FULL", 1).otherwise(0)).alias("full"),
            F.sum(F.when(F.col("state") == "PARTIAL", 1).otherwise(0)).alias("partial"),
            F.sum(F.when(F.col("state") == "EXPIRED", 1).otherwise(0)).alias("expired"),
        )
        .orderBy("symbol")
    )
    summary.show(50, truncate=False)

    total = saved.count()
    full = saved.filter(F.col("state") == "FULL").count()
    print(f"완료 ({elapsed:.1f}s, master={args.master})  총 FVG {total:,}개"
          f"  전체 완충률 {full / total * 100 if total else 0:.1f}%  저장: {args.outdir}")

    spark.stop()


if __name__ == "__main__":
    main()
