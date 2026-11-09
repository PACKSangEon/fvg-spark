"""
메인 수집 스크립트.

사용법:
    python collect_bars.py                        # config.py 기본 설정
    python collect_bars.py --symbols AAPL MSFT    # 심볼 지정
    python collect_bars.py --start 2024-01-01 --end 2024-03-31  # 기간 지정
    python collect_bars.py --dry-run              # 실제 저장 없이 첫 페이지만 확인
    python collect_bars.py --feed sip             # SIP 피드 (전체 거래소 통합)
    python collect_bars.py --outdir data/parquet_sip --feed sip   # 피드별로 따로 저장

진행 상황:
    각 (symbol, month) 조합이 Parquet 파티션 파일로 저장된다.
    이미 저장된 파티션은 건너뛴다 (--force 로 덮어쓰기).
"""
from __future__ import annotations

import argparse
import logging
import os
import sys

from tqdm import tqdm

import config
from collect.alpaca_client import AlpacaClient, monthly_ranges
from collect.parquet_writer import bars_to_dataframe, write_parquet

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("collect_bars")


def partition_exists(output_dir: str, symbol: str, date_prefix: str) -> bool:
    """symbol=X/date=YYYY-MM 으로 시작하는 파티션이 하나라도 있으면 True."""
    base = os.path.join(output_dir, f"symbol={symbol}")
    if not os.path.isdir(base):
        return False
    for name in os.listdir(base):
        if name.startswith(f"date={date_prefix}"):
            return True
    return False


def run(
    symbols: list[str],
    start: str,
    end: str,
    output_dir: str,
    dry_run: bool,
    force: bool,
    feed: str,
) -> None:
    client = AlpacaClient(config.ALPACA_KEY, config.ALPACA_SECRET, config.BASE_URL)
    months = monthly_ranges(start, end)

    tasks = [(sym, s, e) for sym in symbols for s, e in months]
    logger.info("총 %d 작업 (심볼 %d × 월 %d, feed=%s)", len(tasks), len(symbols), len(months), feed)

    skipped = 0
    for symbol, m_start, m_end in tqdm(tasks, unit="month"):
        month_prefix = m_start[:7]  # YYYY-MM

        if not force and not dry_run and partition_exists(output_dir, symbol, month_prefix):
            skipped += 1
            continue

        bars = list(client.iter_bars(symbol, m_start + "T00:00:00Z", m_end + "T23:59:59Z",
                                 config.TIMEFRAME, feed))
        logger.info("%-6s  %s  bars=%d", symbol, month_prefix, len(bars))

        if dry_run:
            if bars:
                logger.info("  첫 번째 바: %s", bars[0])
            continue

        df = bars_to_dataframe(symbol, bars, config.PRICE_SCALE)
        write_parquet(df, output_dir)

    if skipped:
        logger.info("건너뜀: %d 파티션 (이미 존재, --force 로 재수집 가능)", skipped)
    logger.info("완료 → %s", output_dir)


def main() -> None:
    ap = argparse.ArgumentParser(description="Alpaca 1분봉 → Parquet 수집기")
    ap.add_argument("--symbols", nargs="+", default=config.SYMBOLS)
    ap.add_argument("--start",   default=config.START_DATE)
    ap.add_argument("--end",     default=config.END_DATE)
    ap.add_argument("--outdir",  default=config.PARQUET_DIR)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force",   action="store_true", help="기존 파티션 덮어쓰기")
    ap.add_argument("--feed",    default=config.FEED, choices=["iex", "sip"], help="Alpaca 데이터 피드")
    args = ap.parse_args()

    os.makedirs(args.outdir, exist_ok=True)
    run(
        symbols=args.symbols,
        start=args.start,
        end=args.end,
        output_dir=args.outdir,
        dry_run=args.dry_run,
        force=args.force,
        feed=args.feed,
    )


if __name__ == "__main__":
    main()
