"""
PySpark 순수 SQL(Window + lag) 기반 FVG 탐지 + 완충 판정.

detect/fvg_pipeline.py (numpy) 와 같은 규칙을 Python UDF 없이 Spark 연산만으로 구현한다.

탐지 (Window: partitionBy(symbol), orderBy(ts_micros)):
  - BULLISH: lag(high, 2) < low   →  top = low,          bottom = lag(high, 2)
  - BEARISH: lag(low, 2)  > high  →  top = lag(low, 2),  bottom = high

완충 판정 (Window frame: 다음 1 ~ TTL 개 캔들, rowsBetween(1, ttl)):
  - BULLISH: 이후 최저가 min_low,  FULL ⇔ min_low <= bottom,  PARTIAL ⇔ min_low < top
  - BEARISH: 이후 최고가 max_high, FULL ⇔ max_high >= top,    PARTIAL ⇔ max_high > bottom
  - fill_permille = min(1000, 침투 깊이 × 1000 div gap)

numpy 판정과의 관계:
  침투 깊이의 최댓값은 구간 내 최저가(최고가)에서 나오므로, 캔들을 하나씩 도는
  numpy 루프와 같은 state·fill_permille 이 나온다.
  lifetime_bars 는 PARTIAL/EXPIRED 만 같은 값(구간 캔들 수)이고,
  FULL 의 "처음 메워진 캔들 위치"는 Window 집계로 구할 수 없어 null 로 둔다.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

OUTPUT_COLS = [
    "symbol", "type", "origin_ts_micros", "formed_at_micros",
    "top", "bottom", "gap", "gap_centi_bps",
    "state", "fill_permille", "lifetime_bars",
]


def detect_fvgs(bars: DataFrame, ttl: int = 200) -> DataFrame:
    """
    1분봉 DataFrame(symbol, ts_micros, high, low ...) → FVG DataFrame.

    Args:
        bars: fixed-point long 가격의 1분봉 (collect_bars.py 출력 스키마)
        ttl:  완충 판정에 쓰는 이후 캔들 수 (numpy TTL_BARS 와 같은 의미)
    """
    order = Window.partitionBy("symbol").orderBy("ts_micros")
    ahead = order.rowsBetween(1, ttl)

    c = (
        bars.select("symbol", "ts_micros", "high", "low")
        # 탐지: 2개 앞 캔들(c[t-2])
        .withColumn("h2", F.lag("high", 2).over(order))
        .withColumn("l2", F.lag("low", 2).over(order))
        .withColumn("ts2", F.lag("ts_micros", 2).over(order))
        # 완충: 이후 TTL 개 캔들의 최저가·최고가·개수
        .withColumn("min_low_ahead", F.min("low").over(ahead))
        .withColumn("max_high_ahead", F.max("high").over(ahead))
        .withColumn("n_ahead", F.count("low").over(ahead))
    )

    bull = (
        c.filter(F.col("h2") < F.col("low"))
        .select(
            "symbol",
            F.lit("BULLISH").alias("type"),
            F.col("ts2").alias("origin_ts_micros"),
            F.col("ts_micros").alias("formed_at_micros"),
            F.col("low").alias("top"),
            F.col("h2").alias("bottom"),
            F.col("min_low_ahead").alias("x"),   # 이후 최저가
            "n_ahead",
        )
        .withColumn("full", F.col("x").isNotNull() & (F.col("x") <= F.col("bottom")))
        .withColumn("touched", F.col("x").isNotNull() & (F.col("x") < F.col("top")))
        .withColumn("pen", F.col("top") - F.greatest(F.col("x"), F.col("bottom")))
    )

    bear = (
        c.filter(F.col("l2") > F.col("high"))
        .select(
            "symbol",
            F.lit("BEARISH").alias("type"),
            F.col("ts2").alias("origin_ts_micros"),
            F.col("ts_micros").alias("formed_at_micros"),
            F.col("l2").alias("top"),
            F.col("high").alias("bottom"),
            F.col("max_high_ahead").alias("x"),  # 이후 최고가
            "n_ahead",
        )
        .withColumn("full", F.col("x").isNotNull() & (F.col("x") >= F.col("top")))
        .withColumn("touched", F.col("x").isNotNull() & (F.col("x") > F.col("bottom")))
        .withColumn("pen", F.least(F.col("x"), F.col("top")) - F.col("bottom"))
    )

    fvgs = bull.unionByName(bear)

    gap = F.col("top") - F.col("bottom")
    mid = F.expr("(top + bottom) div 2")

    return (
        fvgs
        .withColumn("gap", gap)
        .withColumn(
            "gap_centi_bps",
            F.when(mid > 0, F.expr("(gap * 1000000) div ((top + bottom) div 2)")).otherwise(F.lit(0)).cast("long"),
        )
        .withColumn(
            "state",
            F.when(F.col("full"), "FULL").when(F.col("touched"), "PARTIAL").otherwise("EXPIRED"),
        )
        .withColumn(
            "fill_permille",
            F.when(F.col("full"), 1000)
            .when(F.col("touched"), F.least(F.lit(1000), F.expr("(pen * 1000) div gap")))
            .otherwise(0)
            .cast("int"),
        )
        .withColumn(
            "lifetime_bars",
            F.when(F.col("full"), F.lit(None)).otherwise(F.col("n_ahead")).cast("int"),
        )
        .select(*OUTPUT_COLS)
    )
