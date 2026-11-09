"""
FVG 개수 필터 전후 비교 + 1분봉 데이터 품질 점검.

필터 (누적이 아니라 각각 따로, 마지막 행만 전부 적용):
  1. 필터 없음 (현재 detect_fvgs.py 결과 그대로)
  2. 엔진 최소 갭: gap >= $0.01 (100 units) AND gap_centi_bps >= 200 (2bp)
     → Java 엔진 FvgDetector 기본값 minGapFixed / minGapCentiBps 와 같음
  3. 정규장만: 3개 캔들이 모두 뉴욕 시간 09:30 ~ 16:00 (서머타임 반영)
  4. 같은 거래일: c[t-2] 와 c[t] 가 같은 뉴욕 날짜 (밤사이를 건너뛴 FVG 제외)
  5. 연속 3분: formed_at − origin = 정확히 2분 (중간에 빠진 분봉이 없음)
  6. 2 + 3 + 4 + 5 모두 적용

1분봉 점검:
  - 거래일당 정규장 분봉 수 (완전하면 390개)
  - 장외(프리·애프터마켓) 분봉 비율

사용법:
    python fvg_filter_report.py
    python fvg_filter_report.py --symbol AAPL

출력: data/stats/fvg_filter_report.csv
"""
from __future__ import annotations

import argparse
import os

import pandas as pd
import pyarrow.parquet as pq

import config

ET = "America/New_York"
MIN_GAP_UNITS = 100   # $0.01 (PRICE_SCALE = 10,000)
MIN_GAP_CBPS = 200    # 2bp


def _et(micros: pd.Series) -> pd.Series:
    return pd.to_datetime(micros, unit="us", utc=True).dt.tz_convert(ET)


def _regular(ts: pd.Series) -> pd.Series:
    minutes = ts.dt.hour * 60 + ts.dt.minute
    return (minutes >= 9 * 60 + 30) & (minutes < 16 * 60)


def load(path: str, symbol: str | None) -> pd.DataFrame:
    filters = [("symbol", "=", symbol)] if symbol else None
    df = pq.read_table(path, filters=filters).to_pandas()
    for col in ("symbol", "type", "state", "date"):
        if col in df and hasattr(df[col], "cat"):
            df[col] = df[col].astype(str)
    return df


def bar_quality(bars: pd.DataFrame) -> None:
    ts = _et(bars["ts_micros"])
    reg = _regular(ts)
    day = ts.dt.date
    per_day = bars[reg].groupby([bars.loc[reg, "symbol"], day[reg]]).size()

    print("=== 1분봉 데이터 점검 ===")
    print(f"전체 분봉: {len(bars):,}   정규장: {int(reg.sum()):,} ({reg.mean() * 100:.1f}%)"
          f"   장외: {int((~reg).sum()):,} ({(~reg).mean() * 100:.1f}%)")
    print(f"거래일당 정규장 분봉 수 (완전하면 390): 평균 {per_day.mean():.1f}, "
          f"중앙값 {per_day.median():.0f}, 최소 {per_day.min()}, 최대 {per_day.max()}")
    print("  → 390보다 많이 작으면 거래가 없는 분이 빠진 것 (IEX 피드는 전체 거래의 일부만 포함)\n")


def filter_report(fvgs: pd.DataFrame, bars: pd.DataFrame) -> pd.DataFrame:
    origin = _et(fvgs["origin_ts_micros"])
    formed = _et(fvgs["formed_at_micros"])

    # 가운데 캔들(c[t-1])의 시각: 같은 심볼에서 formed 바로 앞 분봉
    b = bars[["symbol", "ts_micros"]].sort_values(["symbol", "ts_micros"]).reset_index(drop=True)
    b["prev_ts"] = b.groupby("symbol")["ts_micros"].shift(1)
    mid_ts = fvgs[["symbol", "formed_at_micros"]].merge(
        b.rename(columns={"ts_micros": "formed_at_micros"}), on=["symbol", "formed_at_micros"], how="left"
    )["prev_ts"]
    middle = _et(mid_ts.fillna(fvgs["formed_at_micros"]).astype("int64"))

    f_gap = (fvgs["gap"] >= MIN_GAP_UNITS) & (fvgs["gap_centi_bps"] >= MIN_GAP_CBPS)
    f_reg = _regular(origin).values & _regular(middle).values & _regular(formed).values
    f_day = (origin.dt.date.values == formed.dt.date.values)
    f_seq = (fvgs["formed_at_micros"] - fvgs["origin_ts_micros"]) == 120_000_000
    f_all = f_gap & f_reg & f_day & f_seq

    total = len(fvgs)
    rows = []
    for name, mask in [
        ("1. 필터 없음", pd.Series(True, index=fvgs.index)),
        ("2. 엔진 최소 갭 ($0.01 & 2bp)", f_gap),
        ("3. 정규장만", pd.Series(f_reg, index=fvgs.index)),
        ("4. 같은 거래일", pd.Series(f_day, index=fvgs.index)),
        ("5. 연속 3분", f_seq),
        ("6. 2~5 모두", f_all),
    ]:
        sub = fvgs[mask.values]
        n = len(sub)
        rows.append({
            "filter": name,
            "fvg_count": n,
            "pct_of_all": round(n / total * 100, 1) if total else 0,
            "fvg_per_1000_bars": round(n / len(bars) * 1000, 1) if len(bars) else 0,
            "full_rate_pct": round((sub["state"] == "FULL").mean() * 100, 2) if n else 0,
            "median_gap_cbps": int(sub["gap_centi_bps"].median()) if n else 0,
        })
    return pd.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="FVG 필터 전후 비교")
    ap.add_argument("--symbol", default=None)
    args = ap.parse_args()

    bars = load(config.PARQUET_DIR, args.symbol)
    fvgs = load(config.FVG_PARQUET_DIR, args.symbol)

    bar_quality(bars)
    report = filter_report(fvgs, bars)

    print("=== FVG 필터 전후 ===")
    print(report.to_string(index=False))

    os.makedirs("data/stats", exist_ok=True)
    out = f"data/stats/fvg_filter_report_{config.DATASET}.csv"
    report.to_csv(out, index=False)
    print(f"\n저장: {out}")


if __name__ == "__main__":
    main()
