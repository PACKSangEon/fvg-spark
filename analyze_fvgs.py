"""
PySpark SQL 기반 FVG 집계 분석 스크립트.

Python UDF 없이 순수 Spark SQL 집계만 사용하므로 Java 21 / Windows 환경 호환.

사용법:
    python analyze_fvgs.py            # 전체 분석 실행 + CSV 저장
    python analyze_fvgs.py --show     # 터미널에 결과 출력 (저장 없음)
"""
from __future__ import annotations

import argparse
import os
import sys

# Windows PySpark: winutils + 환경변수 설정 (JVM 시작 전)
os.environ["HADOOP_HOME"]        = r"C:\winutils"
os.environ["PATH"]               = r"C:\winutils\bin;" + os.environ.get("PATH", "")
os.environ["PYSPARK_PYTHON"]     = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYSPARK_SUBMIT_ARGS"] = "pyspark-shell"

import config
from analyze.spark_stats import (
    basic_stats, gap_fill_rate, hourly_fill_rate, lifetime_distribution
)


def get_spark():
    from pyspark.sql import SparkSession
    return (
        SparkSession.builder
        .appName("fvg-analyze")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.driver.extraJavaOptions",
                "-Djava.library.path=C:/winutils/bin"
                " --add-opens=java.base/java.lang=ALL-UNNAMED"
                " --add-opens=java.base/java.nio=ALL-UNNAMED"
                " --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
        .getOrCreate()
    )


def save_csv(df, path: str) -> None:
    """Spark DataFrame을 단일 CSV 파일로 저장한다."""
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(path + "_tmp")
    import glob, shutil
    parts = glob.glob(path + "_tmp/part-*.csv")
    if parts:
        shutil.move(parts[0], path)
    shutil.rmtree(path + "_tmp", ignore_errors=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--show", action="store_true", help="터미널 출력만 (CSV 저장 없음)")
    args = ap.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(config.FVG_PARQUET_DIR)
    df.createOrReplaceTempView("fvgs")

    os.makedirs("data/stats", exist_ok=True)

    analyses = [
        ("basic_stats",           basic_stats(df),            "data/stats/basic_stats.csv"),
        ("gap_fill_rate",         gap_fill_rate(df),          "data/stats/gap_fill_rate.csv"),
        ("hourly_fill_rate",      hourly_fill_rate(df),       "data/stats/hourly_fill_rate.csv"),
        ("lifetime_distribution", lifetime_distribution(df),  "data/stats/lifetime_dist.csv"),
    ]

    for name, result, path in analyses:
        print(f"\n=== {name} ===")
        result.show(30, truncate=False)
        if not args.show:
            save_csv(result, path)
            print(f"  저장: {path}")

    spark.stop()
    print("\n분석 완료")


if __name__ == "__main__":
    main()
