"""
FVG 탐지 + 완충 시뮬레이션 파이프라인 (numpy, 심볼 단위 순차 처리).

각 심볼의 전체 1분봉을 하나의 pandas DataFrame으로 받아
순차적으로 3-캔들 패턴 탐지와 완충 시뮬레이션을 수행한다.
detect_fvgs.py 가 ProcessPoolExecutor 로 심볼별 병렬 실행한다.
같은 규칙의 PySpark 버전: detect/spark_detect.py

Java 엔진 규격과의 일치:
  - 가격: fixed-point long (1 unit = $0.0001)
  - BULLISH FVG: c[t-2].high < c[t].low  →  gap = [c[t-2].high, c[t].low]
  - BEARISH FVG: c[t-2].low  > c[t].high →  gap = [c[t].high,   c[t-2].low]
  - fill_permille: 최대 침투 깊이 / 갭 크기 × 1000
  - state: FULL / PARTIAL / EXPIRED (OPEN 은 시뮬레이션 결과에서 제외)
"""
from __future__ import annotations

import pandas as pd

# 완충 시뮬레이션 TTL: formed_at 이후 최대 몇 개 캔들을 확인할 것인가
TTL_BARS = 200

# applyInPandas 에 전달할 DDL 스키마 (Java 엔진 FvgSnapshot 필드 대응)
OUTPUT_SCHEMA = (
    "symbol string, "
    "type string, "
    "origin_ts_micros long, "
    "formed_at_micros long, "
    "top long, "
    "bottom long, "
    "gap long, "
    "gap_centi_bps long, "
    "state string, "
    "fill_permille int, "
    "lifetime_bars int"
)

_OUTPUT_COLS = [
    "symbol", "type", "origin_ts_micros", "formed_at_micros",
    "top", "bottom", "gap", "gap_centi_bps",
    "state", "fill_permille", "lifetime_bars",
]


def _simulate_np(
    fvg_type: str,
    top: int,
    bottom: int,
    highs: "np.ndarray",
    lows: "np.ndarray",
    start: int,
    ttl: int,
) -> tuple[str, int, int]:
    """numpy 배열 기반 완충 시뮬레이션 (pandas .iloc 오버헤드 없음)."""
    gap = top - bottom
    if gap <= 0:
        return "EXPIRED", 0, ttl

    end = min(start + ttl, len(highs))
    max_fill = 0

    if fvg_type == "BULLISH":
        for j in range(start, end):
            c_low = int(lows[j])
            if c_low < top:
                penetration = top - max(c_low, bottom)
                fill = min(1000, penetration * 1000 // gap)
                if fill > max_fill:
                    max_fill = fill
            if c_low <= bottom:
                return "FULL", 1000, j - start + 1
    else:
        for j in range(start, end):
            c_high = int(highs[j])
            if c_high > bottom:
                penetration = min(c_high, top) - bottom
                fill = min(1000, penetration * 1000 // gap)
                if fill > max_fill:
                    max_fill = fill
            if c_high >= top:
                return "FULL", 1000, j - start + 1

    elapsed = end - start
    if max_fill > 0:
        return "PARTIAL", max_fill, elapsed
    return "EXPIRED", 0, elapsed


def process_symbol(key: tuple, pdf: pd.DataFrame) -> pd.DataFrame:
    """
    하나의 심볼 그룹(PySpark applyInPandas 콜백)에서
    FVG 탐지와 완충 시뮬레이션을 수행하고 결과 DataFrame을 반환한다.
    """
    import numpy as np

    symbol = key[0]
    candles = pdf.sort_values("ts_micros").reset_index(drop=True)

    # numpy 배열로 변환 — pandas .iloc 반복 오버헤드 제거
    highs = candles["high"].to_numpy(dtype="int64")
    lows  = candles["low"].to_numpy(dtype="int64")
    ts    = candles["ts_micros"].to_numpy(dtype="int64")
    n = len(highs)

    rows: list[dict] = []

    for i in range(2, n):
        c0_high, c0_low, c0_ts = highs[i-2], lows[i-2], ts[i-2]
        c2_high, c2_low, c2_ts = highs[i],   lows[i],   ts[i]

        if c0_high < c2_low:
            top, bottom = int(c2_low), int(c0_high)
            gap = top - bottom
            mid = (top + bottom) // 2
            state, fill, lifetime = _simulate_np("BULLISH", top, bottom, highs, lows, i + 1, TTL_BARS)
            rows.append({
                "symbol": symbol, "type": "BULLISH",
                "origin_ts_micros": int(c0_ts), "formed_at_micros": int(c2_ts),
                "top": top, "bottom": bottom, "gap": gap,
                "gap_centi_bps": gap * 1_000_000 // mid if mid > 0 else 0,
                "state": state, "fill_permille": fill, "lifetime_bars": lifetime,
            })

        if c0_low > c2_high:
            top, bottom = int(c0_low), int(c2_high)
            gap = top - bottom
            mid = (top + bottom) // 2
            state, fill, lifetime = _simulate_np("BEARISH", top, bottom, highs, lows, i + 1, TTL_BARS)
            rows.append({
                "symbol": symbol, "type": "BEARISH",
                "origin_ts_micros": int(c0_ts), "formed_at_micros": int(c2_ts),
                "top": top, "bottom": bottom, "gap": gap,
                "gap_centi_bps": gap * 1_000_000 // mid if mid > 0 else 0,
                "state": state, "fill_permille": fill, "lifetime_bars": lifetime,
            })

    if not rows:
        return pd.DataFrame(columns=_OUTPUT_COLS)
    return pd.DataFrame(rows, columns=_OUTPUT_COLS)
