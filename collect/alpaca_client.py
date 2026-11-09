"""
Alpaca Market Data API v2 — 1분봉 히스토리 수집 클라이언트.

무료 티어 제약:
  - 기본 IEX 피드 (feed=iex). feed=sip 은 최근 15분을 제외한 과거 데이터 조회 가능
  - 요청당 최대 10,000 바, 페이지네이션으로 전체 기간 수집
  - 429 발생 시 지수 백오프 재시도
"""
from __future__ import annotations

import time
import logging
from datetime import date, timedelta
from typing import Iterator

import requests

logger = logging.getLogger(__name__)

_MAX_RETRIES = 5
_BACKOFF_BASE = 2.0   # seconds


class AlpacaClient:
    """Alpaca Market Data REST 클라이언트."""

    def __init__(self, key: str, secret: str, base_url: str) -> None:
        self._session = requests.Session()
        self._session.headers.update({
            "APCA-API-KEY-ID":     key,
            "APCA-API-SECRET-KEY": secret,
            "Accept":              "application/json",
        })
        self._base = base_url.rstrip("/")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def iter_bars(
        self,
        symbol: str,
        start: str,
        end: str,
        timeframe: str = "1Min",
        feed: str = "iex",
    ) -> Iterator[dict]:
        """
        (symbol, start, end) 구간의 바를 페이지 단위로 스트리밍한다.

        Yields:
            dict: Alpaca 바 딕셔너리 (t, o, h, l, c, v, vw, n)
        """
        params: dict = {
            "start":     start,
            "end":       end,
            "timeframe": timeframe,
            "limit":     10_000,
            "adjustment": "all",   # 분할·배당 조정
            "feed":      feed,     # "iex" (무료) 또는 "sip"
            "sort":      "asc",
        }

        page = 0
        while True:
            data = self._get(f"/stocks/{symbol}/bars", params)
            bars = data.get("bars") or []
            for bar in bars:
                yield bar
            page += 1

            token = data.get("next_page_token")
            if not token:
                break
            params["page_token"] = token
            logger.debug("symbol=%s page=%d token=%s", symbol, page, token[:12])

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _get(self, path: str, params: dict) -> dict:
        url = self._base + path
        for attempt in range(_MAX_RETRIES):
            resp = self._session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = _BACKOFF_BASE ** attempt
                logger.warning("Rate limited — sleeping %.1fs (attempt %d)", wait, attempt + 1)
                time.sleep(wait)
                continue
            resp.raise_for_status()
        raise RuntimeError(f"Max retries exceeded for {url}")


def monthly_ranges(start: str, end: str) -> list[tuple[str, str]]:
    """
    (start, end) 구간을 월 단위로 분할한다.

    Alpaca 무료 티어는 단일 요청으로 수년치를 한번에 가져올 수 있지만,
    월 단위로 쪼개면 중간 실패 시 재시작이 쉽다.
    """
    ranges = []
    cur = date.fromisoformat(start)
    fin = date.fromisoformat(end)

    while cur < fin:
        # 해당 월의 마지막 날
        if cur.month == 12:
            nxt = date(cur.year + 1, 1, 1)
        else:
            nxt = date(cur.year, cur.month + 1, 1)
        chunk_end = min(nxt - timedelta(days=1), fin)
        ranges.append((cur.isoformat(), chunk_end.isoformat()))
        cur = nxt

    return ranges
