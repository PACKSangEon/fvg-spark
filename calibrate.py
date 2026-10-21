"""
임계값 캘리브레이션 스크립트 (순수 Python/pandas — Spark 불필요).

minGap (gap_centi_bps) 과 TTL (lifetime_bars) 두 축으로 그리드 탐색하여
완충률·FVG 수 트레이드오프를 분석한다.

출력:
    data/calibration/mingap_sweep.csv   — minGap 임계값별 통계
    data/calibration/ttl_sweep.csv      — TTL 임계값별 통계
    data/calibration/grid_search.csv    — (minGap, TTL) 2-D 그리드

Java 엔진 연계:
    권장 (minGap, TTL) 조합을 fvg-core FvgAlertCondition.minGap() /
    FvgDetector TTL 설정값으로 직접 피드백한다.
"""
from __future__ import annotations

import os
import pandas as pd
import pyarrow.parquet as pq

import config

# 탐색 그리드
MIN_GAP_VALUES = [0, 100, 200, 300, 500, 700, 1000, 1500, 2000, 3000]  # centi-bps
TTL_VALUES     = [20, 50, 100, 150, 200]                                  # 캔들 수


def load_fvgs() -> pd.DataFrame:
    return pq.read_table(config.FVG_PARQUET_DIR).to_pandas()


def mingap_sweep(df: pd.DataFrame) -> pd.DataFrame:
    """minGap 임계값별 FVG 수·완충률·평균 수명을 집계한다."""
    rows = []
    total = len(df)
    for mg in MIN_GAP_VALUES:
        sub = df[df["gap_centi_bps"] >= mg]
        n = len(sub)
        full = (sub["state"] == "FULL").sum()
        rows.append({
            "min_gap_cbps":   mg,
            "min_gap_usd":    round(mg / 1_000_000 * ((df["top"] + df["bottom"]).mean() / 2 / 10000), 4),
            "fvg_count":      n,
            "fvg_pct":        round(n / total * 100, 1) if total else 0,
            "fill_rate_pct":  round(full / n * 100, 2) if n else 0,
            "avg_lifetime":   round(sub[sub["state"] == "FULL"]["lifetime_bars"].mean(), 1) if full else 0,
        })
    return pd.DataFrame(rows)


def ttl_sweep(df: pd.DataFrame) -> pd.DataFrame:
    """
    TTL을 조정했을 때의 완충률 변화를 예측한다.

    TTL=k : lifetime_bars <= k 인 FULL FVG만 집계 (더 짧은 TTL 시뮬레이션).
    """
    rows = []
    n_full = (df["state"] == "FULL").sum()
    total  = len(df)
    for ttl in TTL_VALUES:
        # TTL 이내에 완충된 FVG
        within = ((df["state"] == "FULL") & (df["lifetime_bars"] <= ttl)).sum()
        # TTL 이내에 완충되지 않은 FVG (expired + partial + full > ttl)
        rows.append({
            "ttl_bars":      ttl,
            "full_within":   int(within),
            "fill_rate_pct": round(within / total * 100, 2) if total else 0,
            "vs_ttl200_pct": round(within / n_full * 100, 1) if n_full else 0,
        })
    return pd.DataFrame(rows)


def grid_search(df: pd.DataFrame) -> pd.DataFrame:
    """(minGap, TTL) 2-D 그리드 탐색."""
    rows = []
    for mg in MIN_GAP_VALUES:
        sub = df[df["gap_centi_bps"] >= mg]
        n = len(sub)
        if n == 0:
            continue
        for ttl in TTL_VALUES:
            full_within = ((sub["state"] == "FULL") & (sub["lifetime_bars"] <= ttl)).sum()
            rows.append({
                "min_gap_cbps":   mg,
                "ttl_bars":       ttl,
                "fvg_count":      n,
                "fill_rate_pct":  round(full_within / n * 100, 2),
                "score":          round(full_within / n * (n / len(df)) * 100, 2),  # 완충률×커버리지
            })
    return pd.DataFrame(rows)


def recommend(grid: pd.DataFrame) -> pd.Series:
    """score(완충률 × 커버리지)가 가장 높은 (minGap, TTL) 조합을 반환한다."""
    return grid.loc[grid["score"].idxmax()]


def main() -> None:
    print("FVG 데이터 로드 중...")
    df = load_fvgs()

    # symbol 컬럼이 category 타입이면 string 으로 변환
    if hasattr(df["symbol"], "cat"):
        df["symbol"] = df["symbol"].astype(str)

    # state 도 string 으로
    if hasattr(df["state"], "cat"):
        df["state"] = df["state"].astype(str)

    print(f"총 FVG: {len(df):,}  심볼: {sorted(df['symbol'].unique())}")

    os.makedirs("data/calibration", exist_ok=True)

    print("\n=== minGap 임계값 탐색 ===")
    mg_df = mingap_sweep(df)
    print(mg_df.to_string(index=False))
    mg_df.to_csv("data/calibration/mingap_sweep.csv", index=False)

    print("\n=== TTL 임계값 탐색 ===")
    ttl_df = ttl_sweep(df)
    print(ttl_df.to_string(index=False))
    ttl_df.to_csv("data/calibration/ttl_sweep.csv", index=False)

    print("\n=== (minGap, TTL) 그리드 탐색 ===")
    grid = grid_search(df)
    best = recommend(grid)
    print(f"권장 파라미터: minGap={best['min_gap_cbps']} cbps  TTL={best['ttl_bars']} 캔들")
    print(f"  완충률={best['fill_rate_pct']}%  FVG수={int(best['fvg_count']):,}  score={best['score']}")
    grid.to_csv("data/calibration/grid_search.csv", index=False)

    print("\n모든 결과 → data/calibration/")


if __name__ == "__main__":
    main()
