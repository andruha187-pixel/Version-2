"""
HTTP к публичным API Polymarket: общий клиент, не чаще HTTP_RPS запросов в
секунду на всех, повторы при 429/5xx и обрывах.
"""
from __future__ import annotations
import asyncio
import logging
import time

import httpx

from config import settings

log = logging.getLogger("http")

_client: httpx.AsyncClient | None = None
_next_at = 0.0
_stats = {"requests": 0, "errors": 0, "last_error": "", "retries": 0}


class ApiError(Exception):
    pass


async def _client_get() -> httpx.AsyncClient:
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=20, headers={"User-Agent": "polymarket-weather-bot"})
    return _client


async def _pace() -> None:
    """Очередь слотов: между чтением и записью _next_at нет await — в asyncio это атомарно."""
    global _next_at
    rps = max(0.2, float(settings.HTTP_RPS))
    now = time.monotonic()
    slot = max(now, _next_at)
    _next_at = slot + 1.0 / rps
    if slot > now:
        await asyncio.sleep(slot - now)


async def get_json(url: str, params: dict | None = None, retries: int = 3):
    """GET → JSON. 4xx (кроме 429) — ApiError сразу; 429/5xx/обрыв — повтор с паузой."""
    last = None
    for attempt in range(retries + 1):
        await _pace()
        _stats["requests"] += 1
        try:
            client = await _client_get()
            resp = await client.get(url, params=params)
        except Exception as exc:  # noqa: BLE001 — сеть: повторим
            last = f"{type(exc).__name__}: {exc}"
        else:
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError as exc:
                    last = f"не JSON: {exc}"
            elif resp.status_code == 429 or resp.status_code >= 500:
                last = f"HTTP {resp.status_code}"
                retry_after = resp.headers.get("retry-after")
                if retry_after:
                    try:
                        await asyncio.sleep(min(60.0, float(retry_after)))
                    except ValueError:
                        pass
            else:
                _stats["errors"] += 1
                _stats["last_error"] = f"HTTP {resp.status_code} {url}"[:200]
                raise ApiError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        if attempt < retries:
            _stats["retries"] += 1
            await asyncio.sleep(min(30.0, 2.0 * 2 ** attempt))
    _stats["errors"] += 1
    _stats["last_error"] = f"{last} {url}"[:200]
    raise ApiError(last or "нет ответа")


def stats() -> dict:
    return dict(_stats)
