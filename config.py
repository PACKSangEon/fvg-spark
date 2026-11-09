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

# symbols.txt 가 있으면 그 목록을 쓴다 (한 줄에 심볼 하나, # 은 주석)
SYMBOLS_FILE = "symbols.txt"
if os.path.exists(SYMBOLS_FILE):
    with open(SYMBOLS_FILE, encoding="utf-8") as _f:
        _syms = [ln.split("#")[0].strip().upper() for ln in _f]
    SYMBOLS = [s for s in _syms if s] or SYMBOLS

# Alpaca 데이터 피드: "iex" (무료, 거래 일부만) 또는 "sip" (전체 거래소 통합, 최근 15분 제외 무료)
FEED = os.environ.get("ALPACA_FEED", "iex")

# 수집 기간
START_DATE = "2022-01-01"
END_DATE   = "2025-12-31"

TIMEFRAME  = "1Min"

# 가격 고정소수점: 1 unit = $0.0001 (Java 엔진 io.fvg.core.Prices 와 동일)
PRICE_SCALE = 10_000

# 데이터셋 전환: FVG_DATASET=iex (기본) 또는 sip
DATASET = os.environ.get("FVG_DATASET", "iex")

# Parquet 저장 경로
if DATASET == "sip":
    PARQUET_DIR          = "data/parquet_sip"
    FVG_PARQUET_DIR      = "data/fvgs_sip_parquet"
    SPARK_FVG_PARQUET_DIR = "data/fvgs_sip_spark_parquet"
else:
    PARQUET_DIR          = "data/parquet"
    FVG_PARQUET_DIR      = "data/fvgs_parquet"
    SPARK_FVG_PARQUET_DIR = "data/fvgs_spark_parquet"
