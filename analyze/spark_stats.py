"""
PySpark SQL 기반 FVG 집계 통계 모듈.

Python UDF / applyInPandas 없이 순수 Spark SQL 연산만 사용 → Java 21 호환.
"""
from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


def basic_stats(df: DataFrame) -> DataFrame:
    """심볼 × 방향별 기본 통계."""
    return (
        df.groupBy("symbol", "type")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("state") == "FULL",    1).otherwise(0)).alias("full"),
            F.sum(F.when(F.col("state") == "PARTIAL", 1).otherwise(0)).alias("partial"),
            F.sum(F.when(F.col("state") == "EXPIRED", 1).otherwise(0)).alias("expired"),
            F.round(F.mean("gap_centi_bps"), 1).alias("avg_gap_cbps"),
            F.round(F.expr("percentile(gap_centi_bps, 0.5)"), 1).alias("med_gap_cbps"),
            F.round(
                F.sum(F.when(F.col("state") == "FULL", 1).otherwise(0)) / F.count("*") * 100, 2
            ).alias("fill_rate_pct"),
            F.round(
                F.mean(F.when(F.col("state") == "FULL", F.col("lifetime_bars"))), 1
            ).alias("avg_lifetime_full"),
        )
        .orderBy("symbol", "type")
    )


def gap_fill_rate(df: DataFrame, bucket_size: int = 200) -> DataFrame:
    """gap_centi_bps 구간별 완충률 — minGap 캘리브레이션 기초 자료."""
    return (
        df.withColumn("gap_bucket",
                      (F.floor(F.col("gap_centi_bps") / bucket_size) * bucket_size).cast("long"))
        .groupBy("gap_bucket")
        .agg(
            F.count("*").alias("total"),
            F.sum(F.when(F.col("state") == "FULL", 1).otherwise(0)).alias("full"),
            F.round(
                F.sum(F.when(F.col("state") == "FULL", 1).otherwise(0)) / F.count("*") * 100, 2
            ).alias("fill_rate_pct"),
            F.round(F.mean("lifetime_bars"), 1).alias("avg_lifetime"),
        )
        .filter(F.col("gap_bucket") <= 5000)   # 극단치 제외
        .orderBy("gap_bucket")
    )


def hourly_fill_rate(df: DataFrame) -> DataFrame:
    """
    뉴욕 장중 시간대별 완충률 (formed_at_micros 기준, UTC → ET -5/-4).
    ET = UTC - 5 (EST) 또는 UTC - 4 (EDT). 단순화: UTC - 5 고정.
    """
    return (
        df.withColumn(
            "hour_et",
            ((F.col("formed_at_micros") / 1_000_000 / 3600).cast("long") % 24 - 5 + 24) % 24
        )
        .groupBy("hour_et")
        .agg(
            F.count("*").alias("total"),
            F.round(
                F.sum(F.when(F.col("state") == "FULL", 1).otherwise(0)) / F.count("*") * 100, 2
            ).alias("fill_rate_pct"),
            F.round(F.mean("gap_centi_bps"), 1).alias("avg_gap_cbps"),
        )
        .filter((F.col("hour_et") >= 9) & (F.col("hour_et") < 16))  # 장중만
        .orderBy("hour_et")
    )


def lifetime_distribution(df: DataFrame) -> DataFrame:
    """lifetime_bars 분포 (FULL 한정, TTL 캘리브레이션 기초 자료)."""
    return (
        df.filter(F.col("state") == "FULL")
        .withColumn("lt_bucket",
                    (F.floor(F.col("lifetime_bars") / 10) * 10).cast("int"))
        .groupBy("lt_bucket")
        .agg(
            F.count("*").alias("count"),
            F.round(F.mean("gap_centi_bps"), 1).alias("avg_gap_cbps"),
        )
        .orderBy("lt_bucket")
    )
