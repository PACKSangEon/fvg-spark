"""
수집 설정. .env 파일 또는 환경변수에서 API 키를 읽는다.
"""
import os
from dotenv import load_dotenv

load_dotenv()

ALPACA_KEY    = os.environ["ALPACA_KEY"]
ALPACA_SECRET = os.environ["ALPACA_SECRET"]
BASE_URL      = "https://data.alpaca.markets/v2"

# 수집 대상 심볼 (NASDAQ 대형주 샘플 — 필요 시 확장)
SYMBOLS = [
    "AAPL", "MSFT", "NVDA", "AMZN", "META",
    "GOOGL", "TSLA", "AVGO", "COST", "NFLX",
]

# 수집 기간
START_DATE = "2022-01-01"
END_DATE   = "2025-12-31"

TIMEFRAME  = "1Min"

# 가격 고정소수점: 1 unit = $0.0001 (Java 엔진 io.fvg.core.Prices 와 동일)
PRICE_SCALE = 10_000

# Parquet 저장 경로
PARQUET_DIR     = "data/parquet"      # 원본 1분봉
FVG_PARQUET_DIR = "data/fvgs_parquet" # FVG 탐지 결과
