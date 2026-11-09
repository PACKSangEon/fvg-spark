"""
numpy 버전(detect_fvgs.py)과 PySpark 버전(detect_fvgs_spark.py) 결과 일치 검증.

비교 키: (symbol, type, formed_at_micros)
비교 열: origin_ts_micros, top, bottom, gap, gap_centi_bps, state, fill_permille
         + lifetime_bars (FULL 제외: Spark 버전은 FULL 의 lifetime 을 계산하지 않음)

사용법:
    python compare_spark_numpy.py
    python compare_spark_numpy.py --numpy data/fvgs_parquet --spark data/fvgs_spark_parquet

종료 코드: 모두 일치하면 0, 하나라도 다르면 1
"""
from __future__ import annotations

import argparse
import sys

import pandas as pd
import pyarrow.parquet as pq

import config

KEY = ["symbol", "type", "formed_at_micros"]
COLS = ["origin_ts_micros", "top", "bottom", "gap", "gap_centi_bps", "state", "fill_permille"]


def load(path: str) -> pd.DataFrame:
    df = pq.read_table(path).to_pandas()
    for col in ("symbol", "type", "state"):
        if hasattr(df[col], "cat"):
            df[col] = df[col].astype(str)
    return df


def compare(np_df: pd.DataFrame, sp_df: pd.DataFrame, show: int = 10) -> bool:
    print(f"numpy FVG: {len(np_df):,}   spark FVG: {len(sp_df):,}")

    m = np_df.merge(sp_df, on=KEY, how="outer", suffixes=("_np", "_sp"), indicator=True)
    only_np = m[m["_merge"] == "left_only"]
    only_sp = m[m["_merge"] == "right_only"]
    both = m[m["_merge"] == "both"]
    print(f"키 일치: {len(both):,}   numpy에만: {len(only_np):,}   spark에만: {len(only_sp):,}")

    ok = only_np.empty and only_sp.empty

    for col in COLS:
        diff = both[both[f"{col}_np"].astype(str) != both[f"{col}_sp"].astype(str)]
        status = "일치" if diff.empty else f"불일치 {len(diff):,}건"
        print(f"  {col:<17} {status}")
        if not diff.empty:
            ok = False
            print(diff[KEY + [f"{col}_np", f"{col}_sp"]].head(show).to_string(index=False))

    not_full = both[both["state_np"] != "FULL"]
    diff = not_full[not_full["lifetime_bars_np"].astype("int64") != not_full["lifetime_bars_sp"].astype("int64")]
    status = "일치" if diff.empty else f"불일치 {len(diff):,}건"
    print(f"  {'lifetime_bars':<17} {status}  (PARTIAL/EXPIRED {len(not_full):,}건 대상)")
    if not diff.empty:
        ok = False
        print(diff[KEY + ["lifetime_bars_np", "lifetime_bars_sp"]].head(show).to_string(index=False))

    print("\n결론:", "numpy 와 Spark 결과가 완전히 일치합니다." if ok else "불일치가 있습니다.")
    return ok


def main() -> None:
    ap = argparse.ArgumentParser(description="numpy vs Spark FVG 결과 비교")
    ap.add_argument("--numpy", default=config.FVG_PARQUET_DIR)
    ap.add_argument("--spark", default="data/fvgs_spark_parquet")
    args = ap.parse_args()

    ok = compare(load(args.numpy), load(args.spark))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
