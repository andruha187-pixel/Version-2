"""
🔮 Прогноз дневного макс/мин по ансамблю моделей (Open-Meteo, бесплатно, без ключа).

Рынок «Highest/Lowest temperature …» резолвится по дневному макс/мин на станции. Ансамбль
(GFS/ICON/ECMWF — набор «участников», слегка разные начальные условия) даёт не одно число, а
распределение дневного макс/мин. Из него — вероятность каждой корзины, которую можно сравнить с
ценой рынка: где рынок отстаёт от прогноза, там возможен перевес. Здесь — только сбор прогнозов
(во времени, чтобы видеть, как рынок сходится к ним); ставок не делает.

Берём почасовую temperature_2m по координатам станции города в её часовом поясе, на FORECAST_DAYS
дней вперёд, по каждому участнику ансамбля считаем дневной макс и мин и пишем их список в базу
(forecasts). Факт для калибровки (смещение модель−станция) берём из METAR, цену рынка — из снимков.
"""
from __future__ import annotations
import asyncio
import logging
import time
from collections import defaultdict

import httpx

from config import settings
from src import cities, storage

log = logging.getLogger("forecast")

_client: httpx.AsyncClient | None = None
_stats = {"runs": 0, "cities": 0, "rows": 0, "last_run": 0, "last_error": "", "ok_cities": 0}


def reset() -> None:
    for k in _stats:
        _stats[k] = "" if k == "last_error" else 0


def stats() -> dict:
    return dict(_stats)


def _members(hourly: dict) -> dict[str, list]:
    """Ключи вида temperature_2m / temperature_2m_member01 → {имя участника: [значения по часам]}."""
    out = {}
    for key, vals in hourly.items():
        if key.startswith("temperature_2m") and isinstance(vals, list):
            out[key] = vals
    return out


def daily_extremes(hourly: dict, kind: str) -> dict[str, list[float]]:
    """{дата (местная) → [дневной макс/мин °C по каждому участнику]}. kind: high — макс, low — мин."""
    times = hourly.get("time") or []
    members = _members(hourly)
    if not times or not members:
        return {}
    dates = [str(t)[:10] for t in times]
    out: dict[str, list[float]] = defaultdict(list)
    for vals in members.values():
        by_date: dict[str, list[float]] = defaultdict(list)
        for d, v in zip(dates, vals):
            if v is not None:
                by_date[d].append(float(v))
        for d, xs in by_date.items():
            if xs:
                out[d].append(max(xs) if kind == "high" else min(xs))
    return out


async def _get_json(url: str, params: dict):
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30, headers={"User-Agent": "polymarket-weather-bot (forecast)"})
    resp = await _client.get(url, params=params)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:150]}")
    return resp.json()


async def forecast_city(city_key: str, lat: float, lon: float, tz: str, run_ts: int) -> int:
    """Прогноз по городу: пишем дневной макс и мин по датам. Возвращает, сколько строк записано."""
    params = {"latitude": lat, "longitude": lon, "hourly": "temperature_2m", "models": settings.FORECAST_MODELS,
              "forecast_days": int(settings.FORECAST_DAYS), "timezone": tz, "temperature_unit": "celsius"}
    data = await _get_json(settings.FORECAST_URL, params)
    hourly = data.get("hourly") if isinstance(data, dict) else None
    if not hourly:
        return 0
    rows = 0
    for kind in ("high", "low"):
        for date, members in daily_extremes(hourly, kind).items():
            if members:
                storage.forecast_add(city_key, date, kind, run_ts, members, "open-meteo")
                rows += 1
    return rows


def _cities() -> list[tuple[str, float, float, str]]:
    out = []
    for ck, (lat, lon) in cities.CITY_LATLON.items():
        tz, _ok = cities.tz_for(ck)
        out.append((ck, lat, lon, tz))
    return out


async def run_once(now: float | None = None) -> int:
    """Один прогон по всем городам. Возвращает, сколько строк записано."""
    now = time.time() if now is None else now
    run_ts = int(now)
    rows = ok = 0
    cs = _cities()
    for ck, lat, lon, tz in cs:
        try:
            n = await forecast_city(ck, lat, lon, tz, run_ts)
        except Exception as exc:  # noqa: BLE001
            _stats["last_error"] = f"{ck}: {exc}"[:200]
            continue
        if n:
            ok += 1
            rows += n
        await asyncio.sleep(float(settings.FORECAST_PAUSE_SEC))
    _stats.update(runs=_stats["runs"] + 1, cities=len(cs), ok_cities=ok, rows=_stats["rows"] + rows,
                  last_run=int(now))
    return rows


async def run_forever() -> None:
    await asyncio.sleep(20)
    while True:
        if settings.FORECAST_ENABLED:
            try:
                await run_once()
            except Exception as exc:  # noqa: BLE001
                _stats["last_error"] = str(exc)[:200]
                log.warning("Прогноз: %s", exc)
        await asyncio.sleep(max(600.0, float(settings.FORECAST_POLL_MIN) * 60))
